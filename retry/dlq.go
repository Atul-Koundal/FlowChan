package retry

import (
	"context"

	fresult "github.com/Atul-Koundal/FlowChan/result"
)

// FailedItem carries an item that exhausted all retry attempts along
// with the last error returned.
type FailedItem[T any] struct {
	Item T
	Err  error
}

// StreamWithDLQ is like Stream but routes permanently failed items to
// a separate dead letter queue channel instead of the main output.
// This lets callers handle failed items separately - log them, store
// them for later reprocessing, or alert on them - without polluting
// the main result stream.
func StreamWithDLQ[In, Out any](
	ctx context.Context,
	in <-chan In,
	maxAttempts int,
	strategy BackoffStrategy,
	fn func(context.Context, In) (Out, error),
) (<-chan fresult.Result[Out], <-chan FailedItem[In]) {
	out := make(chan fresult.Result[Out], 1)
	dlq := make(chan FailedItem[In], 1)

	go func() {
		defer close(out)
		defer close(dlq)

		for {
			select {
			case item, ok := <-in:
				if !ok {
					return
				}

				val, err := DoWithResult(ctx, maxAttempts, strategy, func() (Out, error) {
					return fn(ctx, item)
				})

				if err != nil {
					// all retries exhausted - route to dead letter queue
					select {
					case dlq <- FailedItem[In]{Item: item, Err: err}:
					case <-ctx.Done():
						return
					}
				} else {
					// success - route to main output
					select {
					case out <- fresult.Result[Out]{Value: val}:
					case <-ctx.Done():
						return
					}
				}

			case <-ctx.Done():
				return
			}
		}
	}()

	return out, dlq
}