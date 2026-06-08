package batch

import (
	"context"
	"time"

	ferrors "FlowChan/errors"
)

type Batcher[T any] struct {
	size    int
	timeout time.Duration
}

func New[T any](size int, timeout time.Duration) *Batcher[T] {
	return &Batcher[T]{
		size:    size,
		timeout: timeout,
	}
}

func (b *Batcher[T]) Run(ctx context.Context, in <-chan T) <-chan ferrors.Result[[]T] {
	out := make(chan ferrors.Result[[]T], 1)

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
			out <- ferrors.Result[[]T]{Value: toSend}
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