package resilience

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"hw2/resilience/mocks"

	"go.uber.org/mock/gomock"
)

func TestDefault(t *testing.T) {
	for _, status := range []int{200, 201, 400, 503} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			mock := mocks.NewMockHTTPClient(gomock.NewController(t))
			mockReplies(t, mock, reply{status: status})
			c := testClient(t, mock, WithBudget(testBudget()))
			response, err := c.Do(testRequest(t, t.Context()))
			checkError(t, err, nil)
			if response.StatusCode != status {
				t.Fatalf("status = %d, want %d", response.StatusCode, status)
			}

			checkPayload(t, response, "payload")
		})
	}
}

func TestDefaultTransportError(t *testing.T) {
	mock := mocks.NewMockHTTPClient(gomock.NewController(t))
	want := errors.New("connection refused")
	response, body := httpResponse(503)
	mock.EXPECT().Do(gomock.Any()).Return(response, want)
	c := testClient(t, mock)
	got, err := c.Do(testRequest(t, t.Context()))
	checkError(t, err, want)
	if got != nil || body.closes.Load() != 1 {
		t.Fatal("failed response was not discarded and closed")
	}
}

func TestDefaultTimeouts(t *testing.T) {
	for name, timeout := range map[string]time.Duration{
		"overall":    0,
		"subrequest": 100 * time.Millisecond,
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				mock := mocks.NewMockHTTPClient(gomock.NewController(t))
				trace := mockReplies(t, mock, reply{})
				c := testClient(t, mock, WithBudget(Budget{Latency: time.Second, SubrequestTimeout: timeout}))
				start := time.Now()
				_, err := c.Do(testRequest(t, t.Context()))
				checkError(t, err, context.DeadlineExceeded)
				want := time.Second
				if timeout > 0 {
					want = timeout
				}

				if time.Since(start) != want || trace.contexts[0].Err() == nil {
					t.Fatalf("elapsed = %v, request error = %v", time.Since(start), trace.contexts[0].Err())
				}
			})
		})
	}
}

func TestResponseContextLifetime(t *testing.T) {
	for _, closeWithoutReading := range []bool{false, true} {
		t.Run(strconv.FormatBool(closeWithoutReading), func(t *testing.T) {
			mock := mocks.NewMockHTTPClient(gomock.NewController(t))
			trace := mockReplies(t, mock, reply{status: 200})
			c := testClient(t, mock)
			response, err := c.Do(testRequest(t, t.Context()))
			checkError(t, err, nil)
			if trace.contexts[0].Err() != nil {
				t.Fatal("Do canceled the response before reading its body")
			}

			if closeWithoutReading {
				response.Body.Close()
			} else {
				checkPayload(t, response, "payload")
			}

			checkError(t, trace.contexts[0].Err(), context.Canceled)
			if trace.bodies[0].closes.Load() != 1 {
				t.Fatal("response body was not closed exactly once")
			}
		})
	}
}

func TestInvalidOrCanceledRequest(t *testing.T) {
	for _, name := range []string{"nil request", "nil URL", "canceled"} {
		t.Run(name, func(t *testing.T) {
			mock := mocks.NewMockHTTPClient(gomock.NewController(t))
			c := testClient(t, mock)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			req := testRequest(t, ctx)
			body := &trackingBody{Reader: strings.NewReader("request")}
			req.Body = body
			switch name {
			case "nil request":
				req = nil
			case "nil URL":
				req.URL = nil
			case "canceled":
				cancel()
			}

			response, err := c.Do(req)
			if err == nil || response != nil {
				t.Fatalf("response = %v, error = %v", response, err)
			}

			if req != nil && body.closes.Load() != 1 {
				t.Fatal("unsent request body was not closed")
			}

			if name == "canceled" {
				checkError(t, err, context.Canceled)
			}
		})
	}
}

func TestConcurrentRequestsHaveIndependentBudgets(t *testing.T) {
	const requests = 16
	var counts [requests]atomic.Int32
	mock := mocks.NewMockHTTPClient(gomock.NewController(t))
	mock.EXPECT().Do(gomock.Any()).DoAndReturn(func(req *http.Request) (*http.Response, error) {
		id, err := strconv.Atoi(req.URL.Query().Get("id"))
		if err != nil {
			return nil, err
		}

		status := 200
		if counts[id].Add(1) == 1 {
			status = 503
		}

		response, _ := httpResponse(status)
		return response, nil
	}).Times(requests * 2)
	c := testClient(t, mock,
		WithBudget(Budget{MaxAttempts: 2, Failures: 2}),
		WithConstantRetry(2, 0),
		WithMultiClient(RoundRobin{
			Replicas:       []string{"http://a.test"},
			CircuitBreaker: &CircuitBreakerConfig{Threshold: requests * 2, Cooldown: time.Second},
		}),
	)

	var wg sync.WaitGroup
	for id := range requests {
		req := testRequest(t, t.Context())
		query := req.URL.Query()
		query.Set("id", strconv.Itoa(id))
		req.URL.RawQuery = query.Encode()
		wg.Add(1)
		go func() {
			defer wg.Done()
			response, err := c.Do(req)
			if err != nil {
				t.Errorf("request %d: %v", id, err)
				return
			}

			body, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil || string(body) != "payload" {
				t.Errorf("request %d: body = %q, error = %v", id, body, err)
			}
		}()
	}

	wg.Wait()
	for id := range requests {
		if counts[id].Load() != 2 {
			t.Errorf("request %d: attempts = %d", id, counts[id].Load())
		}
	}
}
