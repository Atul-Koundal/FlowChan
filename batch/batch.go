//For batching two or more messages/data/values flowing around the pipeline
package batch

import (
	"context"
	"time"

	fresult "github.com/Atul-Koundal/FlowChan/result"
)
//A batch stage collects individual items and groups them together before passing downstream. Two triggers flush a batch:

// 1. Size — batch hits N items, send it
// 2.Timeout — not enough items came in but time ran out, send whatever you have


//Yet to work on these comments......
// Batch groups items from an input channel into batches based on a maximum size and a timeout.
// A batch is emitted when it reaches the maximum size, the timeout expires, or the input channel closes.
// This function never emits empty batches. The timeout countdown starts when the first item is added to a new batch.
// To emit batches only when full, set the timeout to -1. Zero timeout is not supported and will panic
type Batcher[T any] struct {
	size    int
	timeout time.Duration
}

//The New function creates a Batcher instance and returns a pointer to it so its methods can be used without copying the struct.
func New[T any](size int, timeout time.Duration) *Batcher[T] {
	return &Batcher[T]{
		size:    size,
		timeout: timeout,
	}
}


//ctx context.Context → used for cancellation.
//in <-chan T → receive-only channel from which items arrive.
func (b *Batcher[T]) Run(ctx context.Context, in <-chan T) <-chan fresult.Result[[]T] {
	out := make(chan fresult.Result[[]T], 1)

	go func() {
		defer close(out)

		ticker := time.NewTicker(b.timeout)
		defer ticker.Stop()

		batch := make([]T, 0, b.size)

		flush := func() {
			if len(batch) == 0 {
				return
			}
			// copy before sending so the next batch
			// doesn't overwrite this one
			toSend := make([]T, len(batch))
			copy(toSend, batch)
			out <- fresult.Result[[]T]{Value: toSend}
			batch = batch[:0] // reset without reallocating
		}

		for {
			select {
			case item, ok := <-in:
				if !ok {
					// input closed — flush whatever is left
					flush()
					return
				}
				batch = append(batch, item)
				if len(batch) >= b.size {
					flush()
					ticker.Reset(b.timeout)
				}

			case <-ticker.C:
				flush() // timeout hit — send partial batch

			case <-ctx.Done():
				flush() // cancelled — flush and exit
				return
			}
		}
	}()

	return out
}
