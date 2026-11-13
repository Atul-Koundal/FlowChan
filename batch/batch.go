// Package batch groups items from a channel into slices, flushing on
// size, on a timer, or on demand. Batchers never block on a consumer
// that has stopped reading once the context is cancelled.
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
	if size < 1 {
		size = 1
	}
	if timeout <= 0 {
		timeout = time.Second
	}
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
// in closes or the context is cancelled. When in closes, a final partial
// batch is flushed before the channel closes. When ctx is cancelled the
// final batch is delivered only if the consumer can take it immediately;
// the goroutine never blocks on an abandoned consumer.
func (b *Batcher[T]) Run(ctx context.Context, in <-chan T) <-chan fresult.Result[[]T] {
	out := make(chan fresult.Result[[]T], 1)

	go func() {
		defer close(out)

		ticker := time.NewTicker(b.timeout)
		defer ticker.Stop()

		batch := make([]T, 0, b.size)

		// flush sends the pending batch. It reports false if ctx was
		// cancelled while waiting for the consumer, in which case the
		// batch is abandoned and the goroutine must exit.
		flush := func() bool {
			if len(batch) == 0 {
				return true
			}
			toSend := make([]T, len(batch))
			copy(toSend, batch)
			select {
			case out <- fresult.Result[[]T]{Value: toSend}:
				batch = batch[:0]
				return true
			case <-ctx.Done():
				return false
			}
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
					if !flush() {
						return
					}
					ticker.Reset(b.timeout)
				}

			case <-ticker.C:
				if !flush() {
					return
				}

			case <-b.flushCh:
				// manual flush triggered by caller
				if !flush() {
					return
				}
				ticker.Reset(b.timeout)

			case <-ctx.Done():
				// best effort: hand over what we have if the consumer
				// is still reading, but never block after cancellation
				if len(batch) > 0 {
					select {
					case out <- fresult.Result[[]T]{Value: append([]T(nil), batch...)}:
					default:
					}
				}
				return
			}
		}
	}()

	return out
}
