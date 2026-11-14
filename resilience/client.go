package resilience

import (
	"context"
	"errors"
	"fmt"
	"net/http"
)

type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

type client struct {
	httpClient HTTPClient
	budget     Budget
	retry      retryPolicy
	multi      multiPolicy
	replicas   []*replica
}

type execution struct {
	ctx      context.Context
	request  *http.Request
	budget   Budget
	attempts int
}

func (c *client) Do(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, errors.New("nil request")
	}

	ctx, cancel := withTimeout(req.Context(), c.budget.Latency)
	run := &execution{ctx: ctx, request: req, budget: c.budget}
	responseOwnsContext := false
	defer func() {
		if !responseOwnsContext {
			cancel()
		}

		if run.attempts == 0 && req.Body != nil {
			req.Body.Close()
		}
	}()

	var response *http.Response
	var err error
	switch c.multi.mode {
	case roundRobinMode:
		response, err = c.roundRobin(run)
	case hedgingMode:
		response, err = c.hedging(run)
	default:
		response, err = c.retryNode(run, nil)
		if errors.Is(err, errNodeAttempts) {
			err = fmt.Errorf("%w: %w", ErrBudgetExhausted, err)
		}
	}

	if err != nil {
		return nil, err
	}

	response.Body = &cancelBody{ReadCloser: response.Body, cancel: cancel}
	responseOwnsContext = true

	return response, nil
}
