package resilience

import (
	"net/http"
	"testing"
	"time"
)

func TestDefaultHTTPClient(t *testing.T) {
	c := testClient(t, nil)
	if _, ok := c.httpClient.(*http.Client); !ok {
		t.Fatal("nil dependency did not create a standard HTTP client")
	}
}

func TestOptionsLastWins(t *testing.T) {
	budget := testBudget()
	c := testClient(t, nil,
		WithBudget(Budget{Latency: -1}),
		WithBudget(budget),
		WithConstantRetry(0, -1),
		WithExponentialRetry(0, -1),
		WithConstantRetry(2, time.Millisecond),
		WithMultiClient(nil),
		WithMultiClient(RoundRobin{
			Replicas: []string{"invalid"}, CircuitBreaker: &CircuitBreakerConfig{Threshold: -1},
		}),
		WithMultiClient(Hedging{Replicas: []string{"http://old.test"}, Delay: time.Second}),
		WithMultiClient(RoundRobin{Replicas: []string{"http://a.test"}}),
	)
	if c.budget != budget || !c.retry.enabled || c.retry.exponential ||
		c.retry.maxAttempts != 2 || c.retry.delay != time.Millisecond {
		t.Fatalf("budget = %+v, retry = %+v", c.budget, c.retry)
	}

	if c.multi.mode != roundRobinMode || c.multi.delay != 0 || c.multi.breaker != nil ||
		len(c.replicas) != 1 || c.replicas[0].origin.Host != "a.test" || c.replicas[0].breaker != nil {
		t.Fatal("last multi-client option did not replace the entire configuration")
	}
}

func TestLastExponentialAndHedgingOptions(t *testing.T) {
	c := testClient(t, nil,
		WithBudget(Budget{Latency: time.Second}),
		WithConstantRetry(2, 0),
		WithExponentialRetry(3, time.Millisecond),
		WithMultiClient(RoundRobin{
			Replicas:       []string{"http://old.test"},
			CircuitBreaker: &CircuitBreakerConfig{Threshold: 1, Cooldown: time.Second},
		}),
		WithMultiClient(Hedging{Replicas: []string{"http://a.test"}, Delay: time.Millisecond}),
	)
	if !c.retry.exponential || c.retry.maxAttempts != 3 || c.multi.mode != hedgingMode ||
		c.multi.breaker != nil || c.replicas[0].breaker != nil {
		t.Fatal("last retry or hedging configuration was not selected")
	}
}

func TestInvalidOptions(t *testing.T) {
	for name, options := range map[string][]Option{
		"negative latency":            {WithBudget(Budget{Latency: -1})},
		"negative subrequest timeout": {WithBudget(Budget{SubrequestTimeout: -1})},
		"zero global attempts":        {WithBudget(Budget{})},
		"negative fast errors":        {WithBudget(Budget{MaxAttempts: 1, FastErrors: -1})},
		"negative failures":           {WithBudget(Budget{MaxAttempts: 1, Failures: -1})},
		"negative timeouts":           {WithBudget(Budget{MaxAttempts: 1, Timeouts: -1})},
		"zero local attempts":         {WithConstantRetry(0, 0)},
		"negative constant delay":     {WithConstantRetry(1, -1)},
		"zero exponential delay":      {WithExponentialRetry(1, 0)},
		"nil multi-client":            {WithMultiClient(nil)},
		"empty replicas":              {WithMultiClient(RoundRobin{})},
		"negative hedging delay":      {WithMultiClient(Hedging{Replicas: []string{"http://a.test"}, Delay: -1})},
		"zero breaker threshold": {WithMultiClient(RoundRobin{
			Replicas: []string{"http://a.test"}, CircuitBreaker: &CircuitBreakerConfig{Cooldown: time.Second},
		})},
		"zero breaker cooldown": {WithMultiClient(RoundRobin{
			Replicas: []string{"http://a.test"}, CircuitBreaker: &CircuitBreakerConfig{Threshold: 1},
		})},
	} {
		t.Run(name, func(t *testing.T) {
			all := append([]Option{WithBudget(testBudget()), WithConstantRetry(3, 0)}, options...)
			c, err := NewClient(nil, all...)
			if err == nil || c != nil {
				t.Fatalf("client = %v, error = %v", c, err)
			}
		})
	}
}

func TestInvalidReplicaOrigins(t *testing.T) {
	for _, address := range []string{
		"http://[", "ftp://a.test", "http:///", "http://a.test/path",
		"http://a.test?q=1", "http://a.test#fragment", "http://user:pass@a.test",
	} {
		t.Run(address, func(t *testing.T) {
			_, err := NewClient(nil, WithBudget(testBudget()), WithMultiClient(RoundRobin{Replicas: []string{address}}))
			if err == nil {
				t.Fatal("invalid origin accepted")
			}
		})
	}

	t.Run("duplicates", func(t *testing.T) {
		_, err := NewClient(nil, WithBudget(testBudget()), WithMultiClient(RoundRobin{
			Replicas: []string{"http://a.test", "http://A.test/"},
		}))
		if err == nil {
			t.Fatal("duplicate origins accepted")
		}
	})
}
