package stream

import (
	"context"
	"sync"

	ferrors "FlowChan/errors"
)

// BackpressureMap processes items concurrently but slows the producer
// when workers are busy, preventing unbounded memory growth.
func BackpressureMap[In, Out any](
	ctx context.Context,
	in <-chan In,
	workers int,
	fn func(context.Context, In) (Out, error),
) <-chan ferrors.Result[Out] {
	// semaphore limits how many items are in-flight at once
	// when all slots are taken, reading from `in` blocks naturally
	sem := make(chan struct{}, workers)
	out := make(chan ferrors.Result[Out], workers)

	var wg sync.WaitGroup

	go func() {
		defer func() {
			wg.Wait()
			close(out)
		}()

		for {
			select {
			case item, ok := <-in:
				if !ok {
					return
				}

				// acquire semaphore slot , blocks if workers are full
				// this IS the backpressure , producer slows down here
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					return
				}

				wg.Add(1)
				go func(it In) {
					defer wg.Done()
					defer func() { <-sem }() // release slot when done

					val, err := fn(ctx, it)
					select {
					case out <- ferrors.Result[Out]{Value: val, Err: err}:
					case <-ctx.Done():
					}
				}(item)

			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}