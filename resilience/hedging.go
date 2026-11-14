package resilience

import (
	"context"
	"errors"
	"net/http"
	"time"
)

func (c *client) hedging(run *execution) (*http.Response, error) {
	if _, ok := run.ctx.Deadline(); !ok {
		return nil, errors.New("hedging requires an overall deadline")
	}

	type result struct {
		index    int
		response *http.Response
		err      error
	}

	results := make(chan result)
	done := make(chan struct{})
	cancels := make([]context.CancelFunc, len(c.replicas))
	winner := -1
	defer func() {
		close(done)
		for i, cancel := range cancels {
			if cancel != nil && i != winner {
				cancel()
			}
		}
	}()

	launch := func(index int) error {
		request, cancel, err := run.prepare(c.replicas[index])
		if err != nil {
			cancel()
			return err
		}

		cancels[index] = cancel
		go func() {
			response, err := c.send(request, cancel, true)
			select {
			case results <- result{index, response, err}:
			case <-done:
				if response != nil {
					response.Body.Close()
				}
			}
		}()

		return nil
	}

	if err := launch(0); err != nil {
		return nil, err
	}

	timer := time.NewTimer(c.multi.delay)
	defer timer.Stop()
	startedRest := false
	launchRest := func() error {
		if startedRest {
			return nil
		}

		startedRest = true
		timer.Stop()
		for index := 1; index < len(c.replicas); index++ {
			if err := launch(index); err != nil {
				return err
			}
		}

		return nil
	}

	for {
		select {
		case r := <-results:
			if run.ctx.Err() != nil {
				if r.response != nil {
					r.response.Body.Close()
				}

				return nil, run.ctx.Err()
			}

			if r.err == nil {
				winner = r.index
				return r.response, nil
			}

			if err := launchRest(); err != nil {
				return nil, err
			}
		case <-timer.C:
			if err := launchRest(); err != nil {
				return nil, err
			}
		case <-run.ctx.Done():
			return nil, run.ctx.Err()
		}
	}
}
