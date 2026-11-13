package batch

import (
	"context"
	"time"

	fresult "github.com/Atul-Koundal/FlowChan/result"
)

// RealtimeBatcher uses a sliding window - the window resets
// every time an item arrives, flushing only when no items
// arrive within the window duration.
type RealtimeBatcher[T any] struct {
	size   int
	window time.Duration
}

// NewRealtime creates a RealtimeBatcher that flushes after window of
// silence or when size items have accumulated (values below 1 / 0 are
// replaced by 1 / 1ms).
func NewRealtime[T any](size int, window time.Duration) *RealtimeBatcher[T] {
	if size < 1 {
		size = 1
	}
	if window <= 0 {
		window = time.Millisecond
	}
	return &RealtimeBatcher[T]{
		size:   size,
		window: window,
	}
}

// Run starts batching items from in. The returned channel closes when
// in closes (after a final flush) or ctx is cancelled (final batch is
// delivered only if the consumer can take it immediately).
func (b *RealtimeBatcher[T]) Run(ctx context.Context, in <-chan T) <-chan fresult.Result[[]T] {
	out := make(chan fresult.Result[[]T], 1)

	go func() {
		defer close(out)

		batch := make([]T, 0, b.size)

		// timer starts nil - only created when first item arrives
		var timer *time.Timer
		var timerCh <-chan time.Time

		// flush reports false if ctx was cancelled while waiting for the
		// consumer; the goroutine must then exit.
		flush := func() bool {
			if len(batch) == 0 {
				return true
			}
			toSend := make([]T, len(batch))
			copy(toSend, batch)
			select {
			case out <- fresult.Result[[]T]{Value: toSend}:
			case <-ctx.Done():
				return false
			}
			batch = batch[:0]

			// stop and nil the timer after flush
			if timer != nil {
				timer.Stop()
				timer = nil
				timerCh = nil
			}
			return true
		}

		for {
			select {
			case item, ok := <-in:
				if !ok {
					flush()
					return
				}
				batch = append(batch, item)

				// reset sliding window on every new item
				if timer != nil {
					timer.Stop()
				}
				timer = time.NewTimer(b.window)
				timerCh = timer.C

				// also flush if size limit hit
				if len(batch) >= b.size {
					if !flush() {
						return
					}
				}

			case <-timerCh:
				// no new items arrived within the window - flush
				if !flush() {
					return
				}

			case <-ctx.Done():
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
