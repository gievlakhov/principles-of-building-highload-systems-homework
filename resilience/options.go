package resilience

import (
	"errors"
	"net/http"
	"time"
)

type Budget struct {
	MaxAttempts       int
	FastErrors        int
	Failures          int
	Timeouts          int
	Latency           time.Duration
	SubrequestTimeout time.Duration
}

type Option func(*client)

func NewClient(httpClient HTTPClient, options ...Option) (*client, error) {
	if httpClient == nil {
		httpClient = &http.Client{}
	}

	c := &client{
		httpClient: httpClient,
		budget:     Budget{MaxAttempts: 1},
		retry:      retryPolicy{maxAttempts: 1},
	}

	for _, option := range options {
		option(c)
	}

	b := c.budget
	if b.Latency < 0 || b.SubrequestTimeout < 0 {
		return nil, errors.New("invalid time budget")
	}

	if c.multi.mode != hedgingMode && (c.retry.enabled || c.multi.mode == roundRobinMode) {
		if b.MaxAttempts <= 0 ||
			b.FastErrors < 0 ||
			b.Failures < 0 ||
			b.Timeouts < 0 ||
			c.retry.maxAttempts <= 0 ||
			c.retry.delay < 0 ||
			c.retry.exponential && c.retry.delay == 0 {
			return nil, errors.New("invalid retry configuration")
		}
	}

	if err := c.initMultiClient(); err != nil {
		return nil, err
	}

	return c, nil
}

func WithBudget(budget Budget) Option {
	return func(c *client) { c.budget = budget }
}
