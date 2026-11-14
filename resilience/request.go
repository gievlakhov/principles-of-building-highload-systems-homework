package resilience

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"
)

func (run *execution) prepare(node *replica) (*http.Request, context.CancelFunc, error) {
	ctx, cancel := withTimeout(run.ctx, run.budget.SubrequestTimeout)
	if err := run.ctx.Err(); err != nil {
		return nil, cancel, err
	}

	request, err := cloneRequest(run.request, ctx, run.attempts)
	if err != nil {
		return nil, cancel, err
	}

	if node != nil {
		request.URL.Scheme = node.origin.Scheme
		request.URL.Host = node.origin.Host
		if request.Host == "" || request.Host == run.request.URL.Host {
			request.Host = node.origin.Host
		}
	}

	run.attempts++
	return request, cancel, nil
}

func (c *client) send(request *http.Request, cancel context.CancelFunc, checkStatus bool) (*http.Response, error) {
	response, err := c.httpClient.Do(request)
	if request.Context().Err() != nil {
		err = request.Context().Err()
	}

	if err == nil && checkStatus && (response.StatusCode < 200 || response.StatusCode >= 300) {
		err = StatusError(response.StatusCode)
	}

	if err != nil {
		if response != nil && response.Body != nil {
			response.Body.Close()
		}

		cancel()
		return nil, err
	}

	response.Body = &cancelBody{ReadCloser: response.Body, cancel: cancel}
	return response, nil
}

func cloneRequest(req *http.Request, ctx context.Context, attempt int) (*http.Request, error) {
	if req.URL == nil {
		return nil, errors.New("nil request URL")
	}

	request := req.Clone(ctx)
	if attempt > 0 && req.Body != nil && req.Body != http.NoBody {
		if req.GetBody == nil {
			return nil, errors.New("cannot replay request: Request.GetBody is nil")
		}

		body, err := req.GetBody()
		if err != nil {
			return nil, err
		}

		request.Body = body
	}

	return request, nil
}

func withTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout > 0 {
		return context.WithTimeout(ctx, timeout)
	}

	return context.WithCancel(ctx)
}

type cancelBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.cancel()
	}

	return n, err
}

func (b *cancelBody) Close() error {
	defer b.cancel()
	return b.ReadCloser.Close()
}
