package resilience

import (
	"context"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"hw2/resilience/mocks"

	"go.uber.org/mock/gomock"
)

type hedgeTrace struct {
	start    time.Time
	ctx      context.Context
	body     *trackingBody
	canceled bool
}

func expectHedge(mock *mocks.MockHTTPClient, host string, step reply) *hedgeTrace {
	trace := &hedgeTrace{}
	matcher := gomock.Cond(func(req *http.Request) bool { return req.URL.Host == host })
	mock.EXPECT().Do(matcher).DoAndReturn(func(req *http.Request) (*http.Response, error) {
		trace.start = time.Now()
		trace.ctx = req.Context()
		if req.Body != nil {
			defer req.Body.Close()
		}

		if step.delay > 0 {
			timer := time.NewTimer(step.delay)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-req.Context().Done():
				trace.canceled = true
				return nil, req.Context().Err()
			}
		}

		if step.status == 0 {
			<-req.Context().Done()
			trace.canceled = true
			return nil, req.Context().Err()
		}

		response, body := httpResponse(step.status)
		trace.body = body
		return response, nil
	})

	return trace
}

func TestHedging(t *testing.T) {
	for name, tt := range map[string]struct {
		replies     []reply
		delay       time.Duration
		budget      Budget
		cancelAfter time.Duration
		starts      []time.Duration
		canceled    []bool
		elapsed     time.Duration
		err         error
	}{
		"fast primary": {
			replies: []reply{{status: 200, delay: time.Second}, {status: 200}},
			delay:   2 * time.Second, starts: []time.Duration{0, -1}, canceled: []bool{false, false}, elapsed: time.Second,
		},
		"fanout and loser cancellation": {
			replies: []reply{{}, {status: 200, delay: 2 * time.Second}, {status: 200, delay: 3 * time.Second}},
			delay:   time.Second, starts: []time.Duration{0, time.Second, time.Second}, canceled: []bool{true, false, true}, elapsed: 3 * time.Second,
		},
		"early error launches the rest only once": {
			replies: []reply{{status: 503, delay: 100 * time.Millisecond}, {status: 400, delay: 100 * time.Millisecond}, {status: 200, delay: 2 * time.Second}},
			delay:   time.Second, starts: []time.Duration{0, 100 * time.Millisecond, 100 * time.Millisecond},
			canceled: []bool{false, false, false}, elapsed: 2100 * time.Millisecond,
		},
		"all errors wait for deadline without retries": {
			replies: []reply{{status: 400}, {status: 503}}, delay: time.Second,
			budget: Budget{Latency: 3 * time.Second}, starts: []time.Duration{0, 0}, canceled: []bool{false, false},
			elapsed: 3 * time.Second, err: context.DeadlineExceeded,
		},
		"deadline before fanout": {
			replies: []reply{{}, {status: 200}}, delay: time.Second,
			budget: Budget{Latency: 500 * time.Millisecond}, starts: []time.Duration{0, -1}, canceled: []bool{true, false},
			elapsed: 500 * time.Millisecond, err: context.DeadlineExceeded,
		},
		"caller cancels every active request": {
			replies: []reply{{}, {}, {}}, delay: time.Millisecond, cancelAfter: 100 * time.Millisecond,
			starts: []time.Duration{0, time.Millisecond, time.Millisecond}, canceled: []bool{true, true, true},
			elapsed: 100 * time.Millisecond, err: context.Canceled,
		},
		"subrequest timeout triggers fanout": {
			replies: []reply{{}, {status: 200, delay: 50 * time.Millisecond}}, delay: time.Second,
			budget: Budget{Latency: 3 * time.Second, SubrequestTimeout: 100 * time.Millisecond},
			starts: []time.Duration{0, 100 * time.Millisecond}, canceled: []bool{true, false}, elapsed: 150 * time.Millisecond,
		},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				mock := mocks.NewMockHTTPClient(gomock.NewController(t))
				hosts := []string{"a.test", "b.test", "c.test"}
				origins := make([]string, len(tt.replies))
				traces := make([]*hedgeTrace, len(tt.replies))
				for i, step := range tt.replies {
					origins[i] = "http://" + hosts[i]
					if tt.starts[i] >= 0 {
						traces[i] = expectHedge(mock, hosts[i], step)
					}
				}

				budget := tt.budget
				if budget.Latency == 0 {
					budget.Latency = 10 * time.Second
				}

				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if tt.cancelAfter > 0 {
					go func() {
						time.Sleep(tt.cancelAfter)
						cancel()
					}()
				}

				c := testClient(t, mock, WithBudget(budget), WithExponentialRetry(10, time.Second),
					WithMultiClient(Hedging{Replicas: origins, Delay: tt.delay}),
				)
				start := time.Now()
				response, err := c.Do(testRequest(t, ctx))
				checkError(t, err, tt.err)
				synctest.Wait()
				if time.Since(start) != tt.elapsed {
					t.Errorf("elapsed = %v, want %v", time.Since(start), tt.elapsed)
				}

				for i, trace := range traces {
					if trace == nil {
						continue
					}

					if trace.start.Sub(start) != tt.starts[i] || trace.canceled != tt.canceled[i] {
						t.Errorf("replica %d: start = %v, canceled = %v", i, trace.start.Sub(start), trace.canceled)
					}

					if trace.body != nil && trace.body.closes.Load() == 0 && trace.ctx.Err() != nil {
						t.Error("winning response context was canceled before body consumption")
					}
				}

				if err == nil {
					checkPayload(t, response, "payload")
				}
			})
		})
	}
}

func TestHedgingClosesUnclaimedSuccessfulResponses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := mocks.NewMockHTTPClient(gomock.NewController(t))
		ready := make(chan struct{}, 2)
		release := make(chan struct{})
		a, aBody := httpResponse(200)
		b, bBody := httpResponse(200)
		responses := map[string]*http.Response{"a.test": a, "b.test": b}
		mock.EXPECT().Do(gomock.Any()).DoAndReturn(func(req *http.Request) (*http.Response, error) {
			ready <- struct{}{}
			<-release
			return responses[req.URL.Host], nil
		}).Times(2)
		go func() {
			<-ready
			<-ready
			close(release)
		}()
		c := testClient(t, mock, WithBudget(Budget{Latency: 10 * time.Second}), WithMultiClient(Hedging{
			Replicas: []string{"http://a.test", "http://b.test"}, Delay: time.Second,
		}))
		response, err := c.Do(testRequest(t, t.Context()))
		checkError(t, err, nil)
		synctest.Wait()
		if aBody.closes.Load()+bBody.closes.Load() != 1 {
			t.Fatal("unclaimed successful response was not closed")
		}

		checkPayload(t, response, "payload")
		if aBody.closes.Load() != 1 || bBody.closes.Load() != 1 {
			t.Fatal("response bodies were leaked or closed multiple times")
		}
	})
}

func TestHedgingRequiresDeadline(t *testing.T) {
	mock := mocks.NewMockHTTPClient(gomock.NewController(t))
	c := testClient(t, mock, WithMultiClient(Hedging{Replicas: []string{"http://a.test"}}))
	response, err := c.Do(testRequest(t, context.Background()))
	if err == nil || response != nil {
		t.Fatalf("response = %v, error = %v", response, err)
	}
}

func TestHedgingUsesRequestDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := mocks.NewMockHTTPClient(gomock.NewController(t))
		expectHedge(mock, "a.test", reply{status: 503})
		c := testClient(t, mock, WithMultiClient(Hedging{Replicas: []string{"http://a.test"}}))
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		start := time.Now()
		_, err := c.Do(testRequest(t, ctx))
		checkError(t, err, context.DeadlineExceeded)
		if time.Since(start) != time.Second {
			t.Fatal("request deadline was not applied")
		}
	})
}
