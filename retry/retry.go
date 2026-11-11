// Package retry provides backoff strategies and retry helpers.
// Use Do for single operations, Wrap to make any stage function retry,
// and Stream for a simple serial retrying stage.
package retry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"

	fresult "github.com/Atul-Koundal/FlowChan/result"
)

// BackoffStrategy takes the attempt number (starting at 0)
// and returns how long to wait before the next attempt.
type BackoffStrategy func(attempt int) time.Duration

// Fixed returns a strategy that always waits the same duration.
func Fixed(wait time.Duration) BackoffStrategy {
	return func(attempt int) time.Duration {
		return wait
	}
}

// ShouldRetryFunc is a function that decides whether an error is
// worth retrying. Return true to retry, false to fail immediately.
type ShouldRetryFunc func(err error) bool

// AlwaysRetry retries on any error. This is the default behaviour.
func AlwaysRetry(_ error) bool { return true }

// NeverRetry never retries. Fails immediately on the first error.
func NeverRetry(_ error) bool { return false }

// expBackoff returns base * 2^attempt capped at max (max <= 0 means no
// cap). It never overflows: once the doubling would exceed the cap or
// the int64 range it returns the cap.
func expBackoff(base, max time.Duration, attempt int) time.Duration {
	if base <= 0 {
		return 0
	}
	limit := max
	if limit <= 0 {
		limit = time.Duration(math.MaxInt64)
	}
	if attempt < 0 {
		attempt = 0
	}
	d := base
	for i := 0; i < attempt; i++ {
		if d >= limit || d > limit/2 {
			return limit
		}
		d *= 2
	}
	if d > limit {
		return limit
	}
	return d
}

// Exponential returns a strategy that doubles the wait on each attempt,
// capped at max.
func Exponential(base, max time.Duration) BackoffStrategy {
	return func(attempt int) time.Duration {
		return expBackoff(base, max, attempt)
	}
}

// ExponentialJitter is Exponential with random jitter so that many
// workers retrying at once do not hammer a recovering system in
// lockstep. The wait is the exponential value reduced by a random
// amount of up to 20%, so it never exceeds max and, unlike additive
// jitter, still varies once the cap has been reached. Recommended for
// production.
func ExponentialJitter(base, max time.Duration) BackoffStrategy {
	return func(attempt int) time.Duration {
		exp := expBackoff(base, max, attempt)
		spread := int64(exp / 5)
		if spread <= 0 {
			return exp
		}
		return exp - time.Duration(rand.Int63n(spread+1))
	}
}

// RetryError is returned when all attempts are exhausted. It wraps
// the last error and records how many attempts were made.
//
//nolint:revive // exported since v0.2.0; renaming would break callers
type RetryError struct {
	Attempts int
	Last     error
}

func (e *RetryError) Error() string {
	return fmt.Sprintf("failed after %d attempts: %v", e.Attempts, e.Last)
}

func (e *RetryError) Unwrap() error {
	return e.Last
}

// Do calls fn up to maxAttempts times (at least once), waiting between
// attempts using strategy. Returns nil on first success. If shouldRetry
// is provided, a non-retryable error stops immediately. A panic in fn is
// returned as a *result.PanicError and is never retried. If ctx is
// cancelled the returned error wraps both ctx.Err() and the last
// failure, so callers can see why it was retrying.
func Do(
	ctx context.Context,
	maxAttempts int,
	strategy BackoffStrategy,
	fn func() error,
	shouldRetry ...ShouldRetryFunc,
) error {
	_, err := DoWithResult(ctx, maxAttempts, strategy, func() (struct{}, error) {
		return struct{}{}, fn()
	}, shouldRetry...)
	return err
}

// DoWithResult is like Do but fn returns a value alongside the error.
func DoWithResult[T any](
	ctx context.Context,
	maxAttempts int,
	strategy BackoffStrategy,
	fn func() (T, error),
	shouldRetry ...ShouldRetryFunc,
) (T, error) {
	retryable := AlwaysRetry
	if len(shouldRetry) > 0 && shouldRetry[0] != nil {
		retryable = shouldRetry[0]
	}
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	if strategy == nil {
		strategy = Fixed(0)
	}

	var zero T
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, errors.Join(err, lastErr)
		}
		val, err := fresult.Catch(fn)
		if err == nil {
			return val, nil
		}
		lastErr = err

		var pe *fresult.PanicError
		if errors.As(err, &pe) || !retryable(err) {
			return zero, err
		}
		if attempt == maxAttempts-1 {
			break
		}

		timer := time.NewTimer(strategy(attempt))
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return zero, errors.Join(ctx.Err(), lastErr)
		}
	}
	return zero, &RetryError{Attempts: maxAttempts, Last: lastErr}
}

// Wrap returns a function that retries fn with backoff. Use it to give
// retry behaviour to any concurrent stage, e.g.
//
//	pipeline.NewStage(8, retry.Wrap(3, retry.ExponentialJitter(time.Second, 30*time.Second), callAPI))
//
// so retries run on the stage's own workers instead of a single goroutine.
func Wrap[In, Out any](
	maxAttempts int,
	strategy BackoffStrategy,
	fn func(context.Context, In) (Out, error),
	shouldRetry ...ShouldRetryFunc,
) func(context.Context, In) (Out, error) {
	return func(ctx context.Context, item In) (Out, error) {
		return DoWithResult(ctx, maxAttempts, strategy, func() (Out, error) {
			return fn(ctx, item)
		}, shouldRetry...)
	}
}

// Stream applies fn to each item from in, retrying failed items with
// backoff. Items are processed one at a time, so a slow retry delays
// everything behind it; for parallel retries use Wrap with a Stage or
// stream.Map. Pass an optional ShouldRetryFunc to stop retrying on
// permanent errors immediately.
func Stream[In, Out any](
	ctx context.Context,
	in <-chan In,
	maxAttempts int,
	strategy BackoffStrategy,
	fn func(context.Context, In) (Out, error),
	shouldRetry ...ShouldRetryFunc,
) <-chan fresult.Result[Out] {
	out := make(chan fresult.Result[Out], 1)
	wrapped := Wrap(maxAttempts, strategy, fn, shouldRetry...)

	go func() {
		defer close(out)
		for {
			select {
			case item, ok := <-in:
				if !ok {
					return
				}
				val, err := wrapped(ctx, item)
				select {
				case out <- fresult.Result[Out]{Value: val, Err: err}:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}
