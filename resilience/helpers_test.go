package resilience

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"hw2/resilience/mocks"

	"go.uber.org/mock/gomock"
)

type trackingBody struct {
	io.Reader
	closes   atomic.Int32
	closeErr error
}

func (b *trackingBody) Close() error {
	b.closes.Add(1)
	return b.closeErr
}

type reply struct {
	status int
	delay  time.Duration
	err    error
}

type callTrace struct {
	starts   []time.Time
	hosts    []string
	contexts []context.Context
	bodies   []*trackingBody
}

func testBudget() Budget {
	return Budget{
		MaxAttempts: 20, FastErrors: 20, Failures: 20, Timeouts: 20,
		Latency: time.Minute, SubrequestTimeout: time.Second,
	}
}

func testRequest(t *testing.T, ctx context.Context) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://service.test/path?q=1", nil)
	if err != nil {
		t.Fatal(err)
	}

	return req
}

func testClient(t *testing.T, httpClient HTTPClient, options ...Option) *client {
	t.Helper()
	c, err := NewClient(httpClient, options...)
	if err != nil {
		t.Fatal(err)
	}

	return c
}

func httpResponse(status int) (*http.Response, *trackingBody) {
	body := &trackingBody{Reader: strings.NewReader("payload")}
	return &http.Response{StatusCode: status, Body: body, Header: make(http.Header)}, body
}

func mockReplies(t *testing.T, mock *mocks.MockHTTPClient, replies ...reply) *callTrace {
	t.Helper()
	trace := &callTrace{}
	mock.EXPECT().Do(gomock.Any()).DoAndReturn(func(req *http.Request) (*http.Response, error) {
		step := replies[len(trace.starts)]
		trace.starts = append(trace.starts, time.Now())
		trace.hosts = append(trace.hosts, req.URL.Host)
		trace.contexts = append(trace.contexts, req.Context())
		if req.Body != nil {
			defer req.Body.Close()
		}

		if step.delay > 0 {
			timer := time.NewTimer(step.delay)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
		}

		if step.err != nil {
			return nil, step.err
		}

		if step.status == 0 {
			<-req.Context().Done()
			return nil, req.Context().Err()
		}

		response, body := httpResponse(step.status)
		trace.bodies = append(trace.bodies, body)
		return response, nil
	}).Times(len(replies))

	return trace
}

func checkError(t *testing.T, got, want error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Fatalf("error = %v, want %v", got, want)
	}
}

func checkPayload(t *testing.T, response *http.Response, want string) {
	t.Helper()
	if response == nil {
		t.Fatal("nil response")
	}

	body, err := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if err != nil || closeErr != nil || string(body) != want {
		t.Fatalf("body = %q, read error = %v, close error = %v; want %q", body, err, closeErr, want)
	}
}

func checkStarts(t *testing.T, trace *callTrace, start time.Time, want ...time.Duration) {
	t.Helper()
	if len(trace.starts) != len(want) {
		t.Fatalf("calls = %d, want %d", len(trace.starts), len(want))
	}

	for i, at := range trace.starts {
		if got := at.Sub(start); got != want[i] {
			t.Errorf("call %d at %v, want %v", i, got, want[i])
		}
	}
}
