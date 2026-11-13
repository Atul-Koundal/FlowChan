package stream

import (
	"context"
	"sync"

	fresult "github.com/Atul-Koundal/FlowChan/result"
)

// BackpressureMap processes items concurrently but slows the producer
// when workers are busy, preventing unbounded memory growth. At most
// workers items are in flight; once all slots are taken, reading from
// in blocks, which pushes back on the producer. A panic in fn becomes a
// *result.PanicError.
func BackpressureMap[In, Out any](
	ctx context.Context,
	in <-chan In,
	workers int,
	fn func(context.Context, In) (Out, error),
) <-chan fresult.Result[Out] {
	if workers < 1 {
		workers = 1
	}
	// semaphore limits how many items are in-flight at once
	sem := make(chan struct{}, workers)
	out := make(chan fresult.Result[Out], workers)

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

				// acquire a slot; this IS the backpressure
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					return
				}

				wg.Add(1)
				go func(it In) {
					defer wg.Done()
					defer func() { <-sem }() // release slot when done

					val, err := fresult.Catch(func() (Out, error) { return fn(ctx, it) })
					select {
					case out <- fresult.Result[Out]{Value: val, Err: err}:
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
