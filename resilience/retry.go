package resilience

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

var ErrBudgetExhausted = errors.New("retry budget exhausted")

var errNodeAttempts = errors.New("node retry attempts exhausted")

type StatusError int

func (e StatusError) Error() string { return fmt.Sprintf("HTTP status %d", e) }

type retryPolicy struct {
	enabled     bool
	maxAttempts int
	delay       time.Duration
	exponential bool
}

func WithConstantRetry(maxAttempts int, delay time.Duration) Option {
	return func(c *client) {
		c.retry = retryPolicy{enabled: true, maxAttempts: maxAttempts, delay: delay}
	}
}

func WithExponentialRetry(maxAttempts int, initialDelay time.Duration) Option {
	return func(c *client) {
		c.retry = retryPolicy{enabled: true, maxAttempts: maxAttempts, delay: initialDelay, exponential: true}
	}
}

func (c *client) retryNode(run *execution, node *replica) (*http.Response, error) {
	maxDelay := time.Duration(1<<63 - 1)
	if run.budget.Latency > 0 {
		maxDelay = run.budget.Latency
	}

	delay := min(c.retry.delay, maxDelay)
	for attempt := 1; ; attempt++ {
		if err := run.ctx.Err(); err != nil {
			return nil, err
		}

		if node != nil && time.Now().Before(node.blockedUntil()) {
			return nil, errNodeAttempts
		}

		request, cancel, err := run.prepare(node)
		if err != nil {
			cancel()
			return nil, err
		}

		response, err := c.send(request, cancel, c.retry.enabled || node != nil)
		if run.ctx.Err() != nil {
			if response != nil {
				response.Body.Close()
			}

			return nil, run.ctx.Err()
		}

		if node != nil && node.breaker != nil {
			node.breaker.record(err)
		}

		if err == nil || !c.retry.enabled && node == nil {
			return response, err
		}

		exhausted, fastError := run.budget.consume(err)
		if exhausted || run.attempts >= run.budget.MaxAttempts {
			return nil, fmt.Errorf("%w: %w", ErrBudgetExhausted, err)
		}

		if attempt == c.retry.maxAttempts || node != nil && time.Now().Before(node.blockedUntil()) {
			return nil, fmt.Errorf("%w: %w", errNodeAttempts, err)
		}

		pause := delay
		if c.retry.exponential && fastError {
			pause = 0
		}

		if err := wait(run.ctx, pause); err != nil {
			return nil, err
		}

		if c.retry.exponential && !fastError {
			if delay > maxDelay/2 {
				delay = maxDelay
			} else {
				delay *= 2
			}
		}
	}
}

func (b *Budget) consume(err error) (exhausted, fastError bool) {
	var status StatusError
	var networkError net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded) || errors.As(err, &networkError) && networkError.Timeout():
		b.Timeouts--
		return b.Timeouts <= 0, false
	case errors.As(err, &status) && status >= 400 && status < 500:
		b.FastErrors--
		return b.FastErrors <= 0, true
	default:
		b.Failures--
		return b.Failures <= 0, false
	}
}

func wait(ctx context.Context, delay time.Duration) error {
	if deadline, ok := ctx.Deadline(); ok && delay >= time.Until(deadline) {
		<-ctx.Done()
		return ctx.Err()
	}

	if delay == 0 {
		return ctx.Err()
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}
