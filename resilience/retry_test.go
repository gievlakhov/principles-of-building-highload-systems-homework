package resilience

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"hw2/resilience/mocks"

	"go.uber.org/mock/gomock"
)

func TestConstantRetry(t *testing.T) {
	for _, delay := range []time.Duration{0, 100 * time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				mock := mocks.NewMockHTTPClient(gomock.NewController(t))
				trace := mockReplies(t, mock, reply{status: 500}, reply{status: 400}, reply{status: 200})
				c := testClient(t, mock, WithBudget(testBudget()), WithConstantRetry(3, delay))
				start := time.Now()
				response, err := c.Do(testRequest(t, t.Context()))
				checkError(t, err, nil)
				checkStarts(t, trace, start, 0, delay, 2*delay)
				checkPayload(t, response, "payload")
				for _, body := range trace.bodies {
					if body.closes.Load() != 1 {
						t.Fatal("an attempt leaked its response body")
					}
				}
			})
		})
	}
}

func TestExponentialRetry(t *testing.T) {
	for name, tt := range map[string]struct {
		replies []reply
		starts  []time.Duration
	}{
		"5xx and timeout": {
			replies: []reply{{status: 500}, {status: 503}, {}, {status: 200}},
			starts:  []time.Duration{0, time.Second, 3 * time.Second, 8 * time.Second},
		},
		"4xx adds no delay or growth": {
			replies: []reply{{status: 500}, {status: 400}, {status: 503}, {status: 200}},
			starts:  []time.Duration{0, time.Second, time.Second, 3 * time.Second},
		},
		"transport error": {
			replies: []reply{{err: errors.New("connection refused")}, {status: 200}},
			starts:  []time.Duration{0, time.Second},
		},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				mock := mocks.NewMockHTTPClient(gomock.NewController(t))
				trace := mockReplies(t, mock, tt.replies...)
				c := testClient(t, mock, WithBudget(testBudget()), WithExponentialRetry(10, time.Second))
				start := time.Now()
				response, err := c.Do(testRequest(t, t.Context()))
				checkError(t, err, nil)
				checkStarts(t, trace, start, tt.starts...)
				checkPayload(t, response, "payload")
			})
		})
	}
}

func TestRetryLimits(t *testing.T) {
	for name, tt := range map[string]struct {
		adjust func(*Budget)
		local  int
		step   reply
		calls  int
		err    error
	}{
		"global attempts":       {func(b *Budget) { b.MaxAttempts = 2 }, 10, reply{status: 500}, 2, StatusError(500)},
		"local attempts":        {func(b *Budget) {}, 2, reply{status: 500}, 2, StatusError(500)},
		"fast errors":           {func(b *Budget) { b.FastErrors = 2 }, 10, reply{status: 400}, 2, StatusError(400)},
		"failures":              {func(b *Budget) { b.Failures = 2 }, 10, reply{status: 503}, 2, StatusError(503)},
		"timeouts":              {func(b *Budget) { b.Timeouts = 2 }, 10, reply{}, 2, context.DeadlineExceeded},
		"zero fast error limit": {func(b *Budget) { b.FastErrors = 0 }, 10, reply{status: 400}, 1, StatusError(400)},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				mock := mocks.NewMockHTTPClient(gomock.NewController(t))
				steps := make([]reply, tt.calls)
				for i := range steps {
					steps[i] = tt.step
				}

				mockReplies(t, mock, steps...)
				budget := testBudget()
				tt.adjust(&budget)
				c := testClient(t, mock, WithBudget(budget), WithConstantRetry(tt.local, 0))
				_, err := c.Do(testRequest(t, t.Context()))
				checkError(t, err, ErrBudgetExhausted)
				checkError(t, err, tt.err)
			})
		})
	}
}

func TestRetrySucceedsOnLastAllowedAttempt(t *testing.T) {
	mock := mocks.NewMockHTTPClient(gomock.NewController(t))
	mockReplies(t, mock, reply{status: 503}, reply{status: 200})
	c := testClient(t, mock, WithBudget(Budget{MaxAttempts: 2, Failures: 2}), WithConstantRetry(2, 0))
	response, err := c.Do(testRequest(t, t.Context()))
	checkError(t, err, nil)
	checkPayload(t, response, "payload")
}

func TestRetryOverallDeadlineIncludesAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := mocks.NewMockHTTPClient(gomock.NewController(t))
		trace := mockReplies(t, mock, reply{status: 503, delay: 600 * time.Millisecond}, reply{})
		budget := testBudget()
		budget.Latency = time.Second
		c := testClient(t, mock, WithBudget(budget), WithConstantRetry(3, 0))
		start := time.Now()
		_, err := c.Do(testRequest(t, t.Context()))
		checkError(t, err, context.DeadlineExceeded)
		checkStarts(t, trace, start, 0, 600*time.Millisecond)
		if time.Since(start) != time.Second {
			t.Fatal("overall deadline was reset between attempts")
		}
	})
}

func TestBackoffInterruptedByDeadlineOrCancellation(t *testing.T) {
	for _, cancelCaller := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelCaller), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				mock := mocks.NewMockHTTPClient(gomock.NewController(t))
				trace := mockReplies(t, mock, reply{status: 503})
				budget := testBudget()
				budget.Latency = 100 * time.Millisecond
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				wantErr := context.DeadlineExceeded
				if cancelCaller {
					wantErr = context.Canceled
					go func() {
						time.Sleep(100 * time.Millisecond)
						cancel()
					}()
					budget.Latency = time.Second
				}

				c := testClient(t, mock, WithBudget(budget), WithExponentialRetry(10, time.Duration(1<<63-1)))
				start := time.Now()
				_, err := c.Do(testRequest(t, ctx))
				checkError(t, err, wantErr)
				checkStarts(t, trace, start, 0)
				if time.Since(start) != 100*time.Millisecond {
					t.Fatal("backoff ignored cancellation or deadline")
				}
			})
		})
	}
}

func TestNoBackoffAfterLastLocalAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := mocks.NewMockHTTPClient(gomock.NewController(t))
		mockReplies(t, mock, reply{status: 503})
		c := testClient(t, mock, WithBudget(testBudget()), WithExponentialRetry(1, time.Second))
		start := time.Now()
		_, err := c.Do(testRequest(t, t.Context()))
		checkError(t, err, ErrBudgetExhausted)
		if time.Since(start) != 0 {
			t.Fatal("waited after the final attempt")
		}
	})
}

func TestBudgetErrorClassification(t *testing.T) {
	for name, tt := range map[string]struct {
		err  error
		want Budget
		fast bool
	}{
		"4xx":               {StatusError(404), Budget{FastErrors: 0, Failures: 1, Timeouts: 1}, true},
		"5xx":               {StatusError(503), Budget{FastErrors: 1, Failures: 0, Timeouts: 1}, false},
		"timeout":           {context.DeadlineExceeded, Budget{FastErrors: 1, Failures: 1, Timeouts: 0}, false},
		"HTTP timeout":      {fmt.Errorf("transport: %w", os.ErrDeadlineExceeded), Budget{FastErrors: 1, Failures: 1, Timeouts: 0}, false},
		"transport failure": {errors.New("connection reset"), Budget{FastErrors: 1, Failures: 0, Timeouts: 1}, false},
	} {
		t.Run(name, func(t *testing.T) {
			budget := Budget{FastErrors: 1, Failures: 1, Timeouts: 1}
			exhausted, fast := budget.consume(tt.err)
			if !exhausted || fast != tt.fast || budget != tt.want {
				t.Fatalf("budget = %+v, exhausted = %v, fast = %v", budget, exhausted, fast)
			}
		})
	}
}
