package resilience

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/iotest"

	"hw2/resilience/mocks"

	"go.uber.org/mock/gomock"
)

func TestRequestReplayAcrossNodes(t *testing.T) {
	mock := mocks.NewMockHTTPClient(gomock.NewController(t))
	var calls []any
	for i, host := range []string{"a.test", "a.test", "b.test"} {
		call := mock.EXPECT().Do(gomock.Any()).DoAndReturn(func(req *http.Request) (*http.Response, error) {
			defer req.Body.Close()
			body, err := io.ReadAll(req.Body)
			if err != nil || string(body) != "request payload" || req.Method != http.MethodPost ||
				req.URL.Host != host || req.Host != host || req.URL.RequestURI() != "/path?q=1" || req.Header.Get("X-Test") != "value" {
				t.Errorf("request = %s %s, host = %s, body = %q, error = %v", req.Method, req.URL, req.Host, body, err)
			}

			status := 503
			if i == 2 {
				status = 200
			}

			response, _ := httpResponse(status)
			return response, nil
		})
		calls = append(calls, call)
	}

	gomock.InOrder(calls...)
	c := testClient(t, mock, WithBudget(testBudget()), WithConstantRetry(2, 0),
		WithMultiClient(RoundRobin{Replicas: []string{"http://a.test", "http://b.test"}}),
	)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://service.test/path?q=1", strings.NewReader("request payload"))
	checkError(t, err, nil)
	req.Header.Set("X-Test", "value")
	response, err := c.Do(req)
	checkError(t, err, nil)
	checkPayload(t, response, "payload")
	if req.URL.Host != "service.test" || req.Host != "service.test" {
		t.Fatal("original request metadata was modified")
	}
}

func TestCustomHostIsPreserved(t *testing.T) {
	mock := mocks.NewMockHTTPClient(gomock.NewController(t))
	mock.EXPECT().Do(gomock.Any()).DoAndReturn(func(req *http.Request) (*http.Response, error) {
		if req.Host != "virtual.test" || req.URL.Host != "a.test" {
			t.Errorf("Host = %q, URL host = %q", req.Host, req.URL.Host)
		}

		response, _ := httpResponse(200)
		return response, nil
	})
	c := testClient(t, mock, WithBudget(testBudget()), WithMultiClient(RoundRobin{Replicas: []string{"http://a.test"}}))
	req := testRequest(t, t.Context())
	req.Host = "virtual.test"
	response, err := c.Do(req)
	checkError(t, err, nil)
	checkPayload(t, response, "payload")
}

func TestRequestReplayErrors(t *testing.T) {
	for _, missingFactory := range []bool{true, false} {
		name := "factory error"
		if missingFactory {
			name = "missing factory"
		}

		t.Run(name, func(t *testing.T) {
			mock := mocks.NewMockHTTPClient(gomock.NewController(t))
			mockReplies(t, mock, reply{status: 503})
			c := testClient(t, mock, WithBudget(testBudget()), WithConstantRetry(3, 0))
			req := testRequest(t, t.Context())
			body := &trackingBody{Reader: strings.NewReader("request")}
			req.Body = body
			want := errors.New("cannot reopen body")
			if !missingFactory {
				req.GetBody = func() (io.ReadCloser, error) { return nil, want }
			}

			response, err := c.Do(req)
			if response != nil || err == nil || body.closes.Load() != 1 {
				t.Fatalf("response = %v, error = %v, closes = %d", response, err, body.closes.Load())
			}

			if missingFactory {
				if !strings.Contains(err.Error(), "GetBody") {
					t.Fatalf("unexpected error: %v", err)
				}
			} else {
				checkError(t, err, want)
			}
		})
	}
}

func TestCancelBody(t *testing.T) {
	wantErr := errors.New("body failure")
	for name, tt := range map[string]struct {
		reader   io.Reader
		read     bool
		closeErr error
		readErr  error
	}{
		"EOF":         {reader: strings.NewReader("payload"), read: true},
		"read error":  {reader: iotest.ErrReader(wantErr), read: true, readErr: wantErr},
		"close error": {reader: strings.NewReader("payload"), closeErr: wantErr},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			original := &trackingBody{Reader: tt.reader, closeErr: tt.closeErr}
			body := &cancelBody{ReadCloser: original, cancel: cancel}
			if tt.read {
				_, err := io.ReadAll(body)
				checkError(t, err, tt.readErr)
				checkError(t, ctx.Err(), context.Canceled)
			}

			checkError(t, body.Close(), tt.closeErr)
			checkError(t, ctx.Err(), context.Canceled)
			if original.closes.Load() != 1 {
				t.Fatal("original body was not closed exactly once")
			}
		})
	}
}
