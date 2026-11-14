package resilience

import (
	"sync"
	"time"
)

type CircuitBreakerConfig struct {
	Threshold int
	Cooldown  time.Duration
}

type circuitBreaker struct {
	mu        sync.Mutex
	threshold int
	cooldown  time.Duration
	failures  int
	until     time.Time
}

func (r *replica) blockedUntil() time.Time {
	if r.breaker == nil {
		return time.Time{}
	}

	r.breaker.mu.Lock()
	defer r.breaker.mu.Unlock()
	return r.breaker.until
}

func (b *circuitBreaker) record(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	if now.Before(b.until) {
		return
	}

	if err == nil {
		b.failures = 0
		return
	}

	b.failures++
	if b.failures >= b.threshold {
		b.until = now.Add(b.cooldown)
		b.failures = 0
	}
}
