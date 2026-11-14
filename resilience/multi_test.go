package resilience

import (
	"context"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"hw2/resilience/mocks"

	"go.uber.org/mock/gomock"
)

func TestRoundRobin(t *testing.T) {
	for name, tt := range map[string]struct {
		replies []reply
		local   Option
		adjust  func(*Budget)
		hosts   []string
		starts  []time.Duration
		err     error
	}{
		"rotation wraps": {
			replies: []reply{{status: 500}, {status: 400}, {status: 503}, {status: 200}},
			hosts:   []string{"a.test", "b.test", "c.test", "a.test"}, starts: []time.Duration{0, 0, 0, 0},
		},
		"local retries and backoff reset per node": {
			replies: []reply{{status: 500}, {status: 503}, {status: 500}, {status: 200}},
			local:   WithExponentialRetry(2, time.Second), hosts: []string{"a.test", "a.test", "b.test", "b.test"},
			starts: []time.Duration{0, time.Second, time.Second, 2 * time.Second},
		},
		"global attempts span nodes": {
			replies: []reply{{status: 500}, {status: 503}, {status: 500}}, local: WithConstantRetry(2, 0),
			adjust: func(b *Budget) { b.MaxAttempts = 3 }, hosts: []string{"a.test", "a.test", "b.test"},
			starts: []time.Duration{0, 0, 0}, err: ErrBudgetExhausted,
		},
		"error budgets span nodes": {
			replies: []reply{{status: 400}, {status: 500}, {status: 404}},
			adjust:  func(b *Budget) { b.FastErrors = 2 }, hosts: []string{"a.test", "b.test", "c.test"},
			starts: []time.Duration{0, 0, 0}, err: ErrBudgetExhausted,
		},
		"subrequest timeout selects next node": {
			replies: []reply{{}, {status: 200}}, hosts: []string{"a.test", "b.test"},
			starts: []time.Duration{0, time.Second},
		},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				mock := mocks.NewMockHTTPClient(gomock.NewController(t))
				trace := mockReplies(t, mock, tt.replies...)
				budget := testBudget()
				if tt.adjust != nil {
					tt.adjust(&budget)
				}

				options := []Option{WithBudget(budget), WithMultiClient(RoundRobin{
					Replicas: []string{"http://a.test", "http://b.test", "http://c.test"},
				})}
				if tt.local != nil {
					options = append(options, tt.local)
				}

				c := testClient(t, mock, options...)
				start := time.Now()
				response, err := c.Do(testRequest(t, t.Context()))
				checkError(t, err, tt.err)
				checkStarts(t, trace, start, tt.starts...)
				if !reflect.DeepEqual(trace.hosts, tt.hosts) {
					t.Fatalf("hosts = %v, want %v", trace.hosts, tt.hosts)
				}

				if err == nil {
					checkPayload(t, response, "payload")
				}
			})
		})
	}
}

func TestCircuitBreakerOpenPools(t *testing.T) {
	for name, tt := range map[string]struct {
		replies  []reply
		cooldown time.Duration
		latency  time.Duration
		starts   []time.Duration
		err      error
	}{
		"wait for earliest recovery": {
			replies:  []reply{{status: 500}, {status: 503}, {status: 200}},
			cooldown: 3 * time.Second, latency: 10 * time.Second,
			starts: []time.Duration{0, 0, 3 * time.Second},
		},
		"overall deadline while all open": {
			replies:  []reply{{status: 500}, {status: 503}},
			cooldown: 5 * time.Second, latency: time.Second,
			starts: []time.Duration{0, 0}, err: context.DeadlineExceeded,
		},
		"4xx and timeout open circuits": {
			replies:  []reply{{status: 400}, {}, {status: 200}},
			cooldown: 3 * time.Second, latency: 10 * time.Second,
			starts: []time.Duration{0, 0, 3 * time.Second},
		},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				mock := mocks.NewMockHTTPClient(gomock.NewController(t))
				trace := mockReplies(t, mock, tt.replies...)
				budget := testBudget()
				budget.Latency = tt.latency
				c := testClient(t, mock, WithBudget(budget), WithMultiClient(RoundRobin{
					Replicas:       []string{"http://a.test", "http://b.test"},
					CircuitBreaker: &CircuitBreakerConfig{Threshold: 1, Cooldown: tt.cooldown},
				}))
				start := time.Now()
				response, err := c.Do(testRequest(t, t.Context()))
				checkError(t, err, tt.err)
				checkStarts(t, trace, start, tt.starts...)
				if err == nil {
					checkPayload(t, response, "payload")
				} else if time.Since(start) != tt.latency {
					t.Fatal("waiting for recovery exceeded the overall deadline")
				}
			})
		})
	}
}

func TestCircuitBreakerPersistsAndRecovers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := mocks.NewMockHTTPClient(gomock.NewController(t))
		trace := mockReplies(t, mock, reply{status: 500}, reply{status: 500}, reply{status: 200}, reply{status: 200}, reply{status: 200})
		c := testClient(t, mock, WithBudget(testBudget()), WithConstantRetry(3, 0), WithMultiClient(RoundRobin{
			Replicas:       []string{"http://a.test", "http://b.test"},
			CircuitBreaker: &CircuitBreakerConfig{Threshold: 2, Cooldown: 2 * time.Second},
		}))
		start := time.Now()
		for range 2 {
			response, err := c.Do(testRequest(t, t.Context()))
			checkError(t, err, nil)
			checkPayload(t, response, "payload")
		}

		time.Sleep(2 * time.Second)
		response, err := c.Do(testRequest(t, t.Context()))
		checkError(t, err, nil)
		checkPayload(t, response, "payload")
		checkStarts(t, trace, start, 0, 0, 0, 0, 2*time.Second)
		want := []string{"a.test", "a.test", "b.test", "b.test", "a.test"}
		if !reflect.DeepEqual(trace.hosts, want) {
			t.Fatalf("hosts = %v, want %v", trace.hosts, want)
		}
	})
}

func TestCircuitBreakerSuccessResetsConsecutiveFailures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := mocks.NewMockHTTPClient(gomock.NewController(t))
		trace := mockReplies(t, mock, reply{status: 500}, reply{status: 200}, reply{status: 500}, reply{status: 200})
		budget := testBudget()
		budget.MaxAttempts = 1
		c := testClient(t, mock, WithBudget(budget), WithMultiClient(RoundRobin{
			Replicas:       []string{"http://a.test"},
			CircuitBreaker: &CircuitBreakerConfig{Threshold: 2, Cooldown: 5 * time.Second},
		}))
		start := time.Now()
		for i := range 4 {
			response, err := c.Do(testRequest(t, t.Context()))
			if i%2 == 0 {
				checkError(t, err, ErrBudgetExhausted)
			} else {
				checkError(t, err, nil)
				checkPayload(t, response, "payload")
			}
		}

		checkStarts(t, trace, start, 0, 0, 0, 0)
	})
}
