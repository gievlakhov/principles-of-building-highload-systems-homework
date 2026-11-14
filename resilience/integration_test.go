package resilience_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"hw2/resilience"
	"hw2/resilience/mocks"

	"go.uber.org/mock/gomock"
)

func readIntegrationResponse(t *testing.T, response *http.Response, want string) {
	t.Helper()
	body, err := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if err != nil || closeErr != nil || string(body) != want {
		t.Fatalf("body = %q, read error = %v, close error = %v; want %q", body, err, closeErr, want)
	}
}

func TestIntegrationRoundRobinRetriesAndCircuitBreaker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := mocks.NewMockHTTPClient(gomock.NewController(t))
		counts := make(map[string]int)
		var hosts []string
		var starts []time.Duration
		start := time.Now()
		mock.EXPECT().Do(gomock.Any()).DoAndReturn(func(req *http.Request) (*http.Response, error) {
			defer req.Body.Close()
			body, err := io.ReadAll(req.Body)
			if err != nil || string(body) != "request payload" || req.Method != http.MethodPost ||
				req.URL.RequestURI() != "/resource?q=1" || req.Header.Get("X-Test") != "value" {
				t.Errorf("request = %s %s, body = %q, error = %v", req.Method, req.URL, body, err)
			}

			host := req.URL.Host
			counts[host]++
			hosts = append(hosts, host)
			starts = append(starts, time.Since(start))
			status := 200
			if host == "a.test" || counts[host] == 1 {
				status = 503
			}

			return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("response"))}, nil
		}).Times(5)
		c, err := resilience.NewClient(mock,
			resilience.WithBudget(resilience.Budget{
				MaxAttempts: 4, FastErrors: 4, Failures: 4, Timeouts: 4,
				Latency: time.Second, SubrequestTimeout: 300 * time.Millisecond,
			}),
			resilience.WithExponentialRetry(3, 100*time.Millisecond),
			resilience.WithMultiClient(resilience.RoundRobin{
				Replicas:       []string{"http://a.test", "http://b.test"},
				CircuitBreaker: &resilience.CircuitBreakerConfig{Threshold: 2, Cooldown: 10 * time.Second},
			}),
		)
		if err != nil {
			t.Fatal(err)
		}

		for range 2 {
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://service.test/resource?q=1", strings.NewReader("request payload"))
			if err != nil {
				t.Fatal(err)
			}

			req.Header.Set("X-Test", "value")
			response, err := c.Do(req)
			if err != nil {
				t.Fatal(err)
			}

			readIntegrationResponse(t, response, "response")
		}

		if !slices.Equal(hosts, []string{"a.test", "a.test", "b.test", "b.test", "b.test"}) ||
			!slices.Equal(starts, []time.Duration{0, 100 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond, 200 * time.Millisecond}) {
			t.Fatalf("hosts = %v, starts = %v", hosts, starts)
		}
	})
}

func TestIntegrationLastOptionsAndGlobalBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := mocks.NewMockHTTPClient(gomock.NewController(t))
		var hosts []string
		var starts []time.Duration
		start := time.Now()
		mock.EXPECT().Do(gomock.Any()).DoAndReturn(func(req *http.Request) (*http.Response, error) {
			hosts = append(hosts, req.URL.Host)
			starts = append(starts, time.Since(start))
			return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader("failed"))}, nil
		}).Times(3)
		c, err := resilience.NewClient(mock,
			resilience.WithBudget(resilience.Budget{MaxAttempts: 1}),
			resilience.WithConstantRetry(1, time.Second),
			resilience.WithMultiClient(resilience.Hedging{Replicas: []string{"http://old.test"}}),
			resilience.WithExponentialRetry(5, time.Second),
			resilience.WithMultiClient(resilience.RoundRobin{Replicas: []string{"http://a.test", "http://b.test"}}),
			resilience.WithConstantRetry(2, 10*time.Millisecond),
			resilience.WithBudget(resilience.Budget{
				MaxAttempts: 3, FastErrors: 3, Failures: 3, Timeouts: 3, Latency: time.Second,
			}),
		)
		if err != nil {
			t.Fatal(err)
		}

		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://service.test/resource", nil)
		if err != nil {
			t.Fatal(err)
		}

		response, err := c.Do(req)
		if response != nil || !errors.Is(err, resilience.ErrBudgetExhausted) || !errors.Is(err, resilience.StatusError(503)) {
			t.Fatalf("response = %v, error = %v", response, err)
		}

		if !slices.Equal(hosts, []string{"a.test", "a.test", "b.test"}) ||
			!slices.Equal(starts, []time.Duration{0, 10 * time.Millisecond, 10 * time.Millisecond}) {
			t.Fatalf("hosts = %v, starts = %v", hosts, starts)
		}
	})
}

func TestIntegrationHedgingCancelsLoserAndStreamsWinner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := mocks.NewMockHTTPClient(gomock.NewController(t))
		primaryCanceled := make(chan struct{})
		releaseBody := make(chan struct{})
		reader, writer := io.Pipe()
		defer writer.Close()
		var winnerContext context.Context
		primary := gomock.Cond(func(req *http.Request) bool { return req.URL.Host == "a.test" })
		secondary := gomock.Cond(func(req *http.Request) bool { return req.URL.Host == "b.test" })
		mock.EXPECT().Do(primary).DoAndReturn(func(req *http.Request) (*http.Response, error) {
			<-req.Context().Done()
			close(primaryCanceled)
			return nil, req.Context().Err()
		})
		mock.EXPECT().Do(secondary).DoAndReturn(func(req *http.Request) (*http.Response, error) {
			winnerContext = req.Context()
			go func() {
				select {
				case <-releaseBody:
					io.WriteString(writer, "winner")
					writer.Close()
				case <-req.Context().Done():
					writer.CloseWithError(req.Context().Err())
				}
			}()

			return &http.Response{StatusCode: 200, Body: reader}, nil
		})
		c, err := resilience.NewClient(mock,
			resilience.WithBudget(resilience.Budget{MaxAttempts: 1, Latency: time.Second}),
			resilience.WithExponentialRetry(5, time.Millisecond),
			resilience.WithMultiClient(resilience.Hedging{
				Replicas: []string{"http://a.test", "http://b.test"}, Delay: 100 * time.Millisecond,
			}),
		)
		if err != nil {
			t.Fatal(err)
		}

		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://service.test/resource", nil)
		if err != nil {
			t.Fatal(err)
		}

		start := time.Now()
		response, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}

		defer response.Body.Close()
		<-primaryCanceled
		if winnerContext.Err() != nil || time.Since(start) != 100*time.Millisecond {
			t.Fatalf("winner context error = %v, elapsed = %v", winnerContext.Err(), time.Since(start))
		}

		close(releaseBody)
		readIntegrationResponse(t, response, "winner")
		synctest.Wait()
		if !errors.Is(winnerContext.Err(), context.Canceled) {
			t.Fatalf("winner context was not released: %v", winnerContext.Err())
		}
	})
}
