// Package retry provides backoff strategies and retry helpers.
// Use Do for single operations, Stream to add retry to a pipeline stage.
package retry

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	fresult "github.com/Atul-Koundal/FlowChan/result"
)
//Backoff strategy is used when the probe fails to establish a connection or loses an existing connection
//The probe tries to reestablish a connection after one second, two seconds, then four seconds, and so on
//The backoff strategy remains in place until a successful login occurs.


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

// Exponential returns a strategy that doubles the wait on each attempt,
// capped at max.
func Exponential(base, max time.Duration) BackoffStrategy {
	return func(attempt int) time.Duration {
		wait := base * time.Duration(math.Pow(2, float64(attempt)))
		if wait > max {
			return max
		}
		return wait
	}
}

// ExponentialJitter doubles the wait time on every attempt
// and adds random jitter to prevent thundering herd.
// All workers retrying at the same instant hammers a
// recovering system - jitter spreads them out.

// ExponentialJitter returns an exponential strategy with random jitter.
// Recommended for production - prevents thundering herd.
func ExponentialJitter(base, max time.Duration) BackoffStrategy {
	return func(attempt int) time.Duration {
		exp := base * time.Duration(math.Pow(2, float64(attempt)))
		if exp > max {
			exp = max
		}
		// jitter is up to 20% of exp, but final value must not exceed max
		jitter := time.Duration(rand.Int63n(int64(exp / 5)))
		wait := exp + jitter
		if wait > max {
			return max
		}
		return wait
	}
}

// RetryError is returned when all attempts are exhausted. It wraps
// the last error and records how many attempts were made.
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

// Do calls fn up to maxAttempts times, waiting between attempts using
// strategy. Returns nil on first success. If shouldRetry is provided,
// a non-retryable error stops immediately without further attempts.
// Respects context cancellation during wait periods.
func Do(
	ctx context.Context,
	maxAttempts int,
	strategy BackoffStrategy,
	fn func() error,
	shouldRetry ...ShouldRetryFunc,
) error {
	retryable := AlwaysRetry
	if len(shouldRetry) > 0 && shouldRetry[0] != nil {
		retryable = shouldRetry[0]
	}

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		// stop immediately if error is not retryable
		if !retryable(lastErr) {
			return lastErr
		}
		if attempt == maxAttempts-1 {
			break
		}
		wait := strategy(attempt)
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return &RetryError{Attempts: maxAttempts, Last: lastErr}
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

	var lastErr error
	var zero T

	for attempt := 0; attempt < maxAttempts; attempt++ {
		val, err := fn()
		if err == nil {
			return val, nil
		}
		lastErr = err
		if !retryable(lastErr) {
			return zero, lastErr
		}
		if attempt == maxAttempts-1 {
			break
		}
		wait := strategy(attempt)
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return zero, ctx.Err()
		}
	}
	return zero, &RetryError{Attempts: maxAttempts, Last: lastErr}
}

// Stream applies fn to each item from in, retrying failed items with
// backoff. Pass an optional ShouldRetryFunc to stop retrying on
// permanent errors immediately without exhausting all attempts.
func Stream[In, Out any](
	ctx context.Context,
	in <-chan In,
	maxAttempts int,
	strategy BackoffStrategy,
	fn func(context.Context, In) (Out, error),
	shouldRetry ...ShouldRetryFunc,
) <-chan fresult.Result[Out] {
	out := make(chan fresult.Result[Out], 1)

	go func() {
		defer close(out)
		for {
			select {
			case item, ok := <-in:
				if !ok {
					return
				}
				val, err := DoWithResult(ctx, maxAttempts, strategy, func() (Out, error) {
					return fn(ctx, item)
				}, shouldRetry...)

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