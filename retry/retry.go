package retry

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	ferrors "FlowChan/errors"
)
//Backoff strategy is used when the probe fails to establish a connection or loses an existing connection
//The probe tries to reestablish a connection after one second, two seconds, then four seconds, and so on
//The backoff strategy remains in place until a successful login occurs.


// BackoffStrategy takes the attempt number (starting at 0)
// and returns how long to wait before the next attempt.
type BackoffStrategy func(attempt int) time.Duration

// Fixed waits the same duration between every attempt.
func Fixed(wait time.Duration) BackoffStrategy {
	return func(attempt int) time.Duration {
		return wait
	}
}

// Exponential doubles the wait time on every attempt,
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

// RetryError wraps the last error with attempt count context.
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

// Do runs fn up to maxAttempts times, waiting between attempts
// using the provided strategy. Returns nil on first success.
// Respects context cancellation during wait periods.
func Do(ctx context.Context, maxAttempts int, strategy BackoffStrategy, fn func() error) error {
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil // success
		}

		// last attempt - no point waiting
		if attempt == maxAttempts-1 {
			break
		}

		wait := strategy(attempt)
		select {
		case <-time.After(wait):
			// wait complete, try again
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return &RetryError{Attempts: maxAttempts, Last: lastErr}
}

// DoWithResult runs fn up to maxAttempts times and returns
// the value on success. Same backoff behaviour as Do.
func DoWithResult[T any](ctx context.Context, maxAttempts int, strategy BackoffStrategy, fn func() (T, error)) (T, error) {
	var lastErr error
	var zero T

	for attempt := 0; attempt < maxAttempts; attempt++ {
		val, err := fn()
		if err == nil {
			return val, nil
		}
		lastErr = err

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

// Stream applies fn to each item from in, retrying failed
// items with backoff before emitting an error downstream.
// Successful results and errors both flow through Result[T].
func Stream[In, Out any](
	ctx context.Context,
	in <-chan In,
	maxAttempts int,
	strategy BackoffStrategy,
	fn func(context.Context, In) (Out, error),
) <-chan ferrors.Result[Out] {
	out := make(chan ferrors.Result[Out], 1)

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
				})

				select {
				case out <- ferrors.Result[Out]{Value: val, Err: err}:
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