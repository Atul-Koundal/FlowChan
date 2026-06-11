package batch

import (
	"context"
	"time"

	ferrors "FlowChan/errors"
)

// RealtimeBatcher uses a sliding window - the window resets
// every time an item arrives, flushing only when no items
// arrive within the window duration.
type RealtimeBatcher[T any] struct {
	size   int
	window time.Duration
}

func NewRealtime[T any](size int, window time.Duration) *RealtimeBatcher[T] {
	return &RealtimeBatcher[T]{
		size:   size,
		window: window,
	}
}

func (b *RealtimeBatcher[T]) Run(ctx context.Context, in <-chan T) <-chan ferrors.Result[[]T] {
	out := make(chan ferrors.Result[[]T], 1)

	go func() {
		defer close(out)

		batch := make([]T, 0, b.size)

		// timer starts nil - only created when first item arrives
		var timer *time.Timer
		var timerCh <-chan time.Time

		flush := func() {
			if len(batch) == 0 {
				return
			}
			toSend := make([]T, len(batch))
			copy(toSend, batch)
			out <- ferrors.Result[[]T]{Value: toSend}
			batch = batch[:0]

			// stop and nil the timer after flush
			if timer != nil {
				timer.Stop()
				timer = nil
				timerCh = nil
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

				// reset sliding window on every new item
				if timer != nil {
					timer.Stop()
				}
				timer = time.NewTimer(b.window)
				timerCh = timer.C

				// also flush if size limit hit
				if len(batch) >= b.size {
					flush()
				}

			case <-timerCh:
				// no new items arrived within the window - flush
				flush()

			case <-ctx.Done():
				flush()
				return
			}
		}
	}()

	return out
}