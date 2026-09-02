package batch

import (
	"context"
	"time"

	fresult "github.com/Atul-Koundal/FlowChan/result"
)

// Batcher groups items from an input channel into batches, flushing
// when either the batch reaches size items or timeout elapses,
// whichever comes first. Call Flush() to trigger an immediate flush.
type Batcher[T any] struct {
	size    int
	timeout time.Duration
	flushCh chan struct{}
}

// New creates a Batcher that flushes when size items accumulate or
// timeout elapses. Both triggers are active simultaneously.
func New[T any](size int, timeout time.Duration) *Batcher[T] {
	return &Batcher[T]{
		size:    size,
		timeout: timeout,
		flushCh: make(chan struct{}, 1),
	}
}

// Flush triggers an immediate flush of the current batch regardless
// of size or timeout. Non-blocking - if a flush is already pending
// it has no additional effect.
func (b *Batcher[T]) Flush() {
	select {
	case b.flushCh <- struct{}{}:
	default: // flush already pending, skip
	}
}

// Run starts batching items from in. The returned channel closes when
// in closes or the context is cancelled. A final partial batch is
// always flushed before the channel closes.
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
			toSend := make([]T, len(batch))
			copy(toSend, batch)
			out <- fresult.Result[[]T]{Value: toSend}
			batch = batch[:0]
		}

		for {
			select {
			case item, ok := <-in:
				if !ok {
					flush()
					return
				}
				batch = append(batch, item)
				if len(batch) >= b.size {
					flush()
					ticker.Reset(b.timeout)
				}

			case <-ticker.C:
				flush()

			case <-b.flushCh:
				// manual flush triggered by caller
				flush()
				ticker.Reset(b.timeout)

			case <-ctx.Done():
				flush()
				return
			}
		}
	}()

	return out
}