package tape9

import (
	"context"
	"errors"
	"io"
	"net"
	"time"
)

const (
	defaultRequestTimeout = 30 * time.Second
	defaultMaxRetryTime   = 2 * time.Minute
	defaultRetryBase      = 200 * time.Millisecond
	defaultRetryMax       = 5 * time.Second
)

type retryConfig struct {
	base         time.Duration
	max          time.Duration
	maxRetryTime time.Duration
}

func (c *Client) retryConfig() retryConfig {
	return retryConfig{
		base:         c.retryBase,
		max:          c.retryMax,
		maxRetryTime: c.maxRetryTime,
	}
}

func (c *Client) doWithRetry(ctx context.Context, attempt func(context.Context) error) error {
	return doWithRetry(ctx, c.retryConfig(), attempt)
}

func doWithRetry(ctx context.Context, cfg retryConfig, attempt func(context.Context) error) error {
	retryStart := time.Time{}
	var lastErr error
	backoff := cfg.base

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		attemptStarted := time.Now()
		attemptCtx := ctx
		cancelAttempt := func() {}
		// The first attempt keeps its normal request timeout. Later attempts
		// cannot outlive the retry budget already consumed by failures and waits.
		if !retryStart.IsZero() {
			if !time.Now().Before(retryStart.Add(cfg.maxRetryTime)) {
				return lastErr
			}
			attemptCtx, cancelAttempt = context.WithDeadline(ctx, retryStart.Add(cfg.maxRetryTime))
		}
		err := attempt(attemptCtx)
		cancelAttempt()
		if err == nil {
			return nil
		}

		lastErr = err
		retryAfter, ok := shouldRetry(err)
		if !ok {
			return err
		}

		if retryStart.IsZero() {
			retryStart = attemptStarted
			backoff = cfg.base
		}

		remainingBudget := time.Until(retryStart.Add(cfg.maxRetryTime))
		if remainingBudget <= 0 {
			return err
		}

		sleep := backoff
		if retryAfter > sleep {
			sleep = retryAfter
		}
		if retryAfter <= 0 && sleep > cfg.max {
			sleep = cfg.max
		}
		if sleep > remainingBudget {
			return err
		}

		if err := sleepContext(ctx, sleep); err != nil {
			return err
		}

		backoff *= 2
		if backoff > cfg.max {
			backoff = cfg.max
		}
	}
}

func shouldRetry(err error) (time.Duration, bool) {
	if err == nil {
		return 0, false
	}

	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return 0, false
	}

	var retryable *RetryableError
	if errors.As(err, &retryable) {
		return retryable.RetryAfter, true
	}

	if isTransportError(err) {
		return 0, true
	}

	return 0, false
}

func isTransportError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
