package resilience

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type MultiClient interface {
	policy() multiPolicy
}

type RoundRobin struct {
	Replicas       []string
	CircuitBreaker *CircuitBreakerConfig
}

func (s RoundRobin) policy() multiPolicy {
	return multiPolicy{mode: roundRobinMode, replicas: s.Replicas, breaker: s.CircuitBreaker}
}

type Hedging struct {
	Replicas []string
	Delay    time.Duration
}

func (s Hedging) policy() multiPolicy {
	return multiPolicy{mode: hedgingMode, replicas: s.Replicas, delay: s.Delay}
}

func WithMultiClient(strategy MultiClient) Option {
	return func(c *client) {
		c.multi = multiPolicy{mode: invalidMultiMode}
		if strategy != nil {
			c.multi = strategy.policy()
		}
	}
}

type multiMode uint8

const (
	singleMode multiMode = iota
	roundRobinMode
	hedgingMode
	invalidMultiMode
)

type multiPolicy struct {
	mode     multiMode
	replicas []string
	delay    time.Duration
	breaker  *CircuitBreakerConfig
}

type replica struct {
	origin  *url.URL
	breaker *circuitBreaker
}

func (c *client) initMultiClient() error {
	if c.multi.mode == singleMode {
		return nil
	}

	if c.multi.mode == invalidMultiMode || len(c.multi.replicas) == 0 || c.multi.delay < 0 {
		return errors.New("invalid multi-client configuration")
	}

	breaker := c.multi.breaker
	if breaker != nil && (breaker.Threshold <= 0 || breaker.Cooldown <= 0) {
		return errors.New("invalid circuit breaker configuration")
	}

	seen := make(map[string]bool)
	for _, address := range c.multi.replicas {
		origin, err := url.Parse(address)
		if err != nil {
			return fmt.Errorf("invalid replica origin %q: %w", address, err)
		}

		if (origin.Scheme != "http" && origin.Scheme != "https") ||
			origin.Hostname() == "" ||
			(origin.Path != "" && origin.Path != "/") ||
			origin.RawQuery != "" ||
			origin.Fragment != "" ||
			origin.User != nil {
			return fmt.Errorf("replica must be an HTTP origin: %q", address)
		}

		key := origin.Scheme + "://" + strings.ToLower(origin.Host)
		if seen[key] {
			return fmt.Errorf("duplicate replica: %q", address)
		}

		seen[key] = true
		node := &replica{origin: origin}
		if breaker != nil {
			node.breaker = &circuitBreaker{threshold: breaker.Threshold, cooldown: breaker.Cooldown}
		}

		c.replicas = append(c.replicas, node)
	}

	return nil
}

func (c *client) roundRobin(run *execution) (*http.Response, error) {
	next := 0
	for {
		node, index, err := c.nextReplica(run.ctx, next)
		if err != nil {
			return nil, err
		}

		next = (index + 1) % len(c.replicas)
		response, err := c.retryNode(run, node)
		if !errors.Is(err, errNodeAttempts) {
			return response, err
		}
	}
}

func (c *client) nextReplica(ctx context.Context, start int) (*replica, int, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}

		var earliest time.Time
		for offset := range c.replicas {
			index := (start + offset) % len(c.replicas)
			node := c.replicas[index]
			until := node.blockedUntil()
			if !time.Now().Before(until) {
				return node, index, nil
			}

			if earliest.IsZero() || until.Before(earliest) {
				earliest = until
			}
		}

		if err := wait(ctx, time.Until(earliest)); err != nil {
			return nil, 0, err
		}
	}
}
