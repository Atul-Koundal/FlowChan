// Package ordered implements concurrent processing that emits results
// in input order. It is shared by stream.OrderedMap and
// pipeline.OrderedStage so the tricky logic exists exactly once.
package ordered

import (
	"context"
	"sync"

	fresult "github.com/Atul-Koundal/FlowChan/result"
)

// windowFactor bounds how far ahead of the oldest unfinished item the
// workers may run, as a multiple of the worker count. Without a bound,
// one slow item at the head of the line lets the reorder buffer grow
// without limit.
const windowFactor = 4

type job[T any] struct {
	idx int
	val T
}

type indexed[T any] struct {
	idx int
	res fresult.Result[T]
}

// Run applies fn to every item of in using workers goroutines and
// returns results in the same order as the input. A panic in fn becomes
// a *result.PanicError for that item. All goroutines exit when ctx is
// cancelled, even if the consumer stops reading.
func Run[In, Out any](
	ctx context.Context,
	in <-chan In,
	workers int,
	fn func(context.Context, In) (Out, error),
) <-chan fresult.Result[Out] {
	if workers < 1 {
		workers = 1
	}
	window := make(chan struct{}, workers*windowFactor)
	jobs := make(chan job[In], workers)

	// tag each input with its sequence number
	go func() {
		defer close(jobs)
		for i := 0; ; {
			select {
			case v, ok := <-in:
				if !ok {
					return
				}
				select {
				case window <- struct{}{}:
				case <-ctx.Done():
					return
				}
				select {
				case jobs <- job[In]{idx: i, val: v}:
				case <-ctx.Done():
					return
				}
				i++
			case <-ctx.Done():
				return
			}
		}
	}()

	// process concurrently
	raw := make(chan indexed[Out], workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case j, ok := <-jobs:
					if !ok {
						return
					}
					val, err := fresult.Catch(func() (Out, error) { return fn(ctx, j.val) })
					select {
					case raw <- indexed[Out]{idx: j.idx, res: fresult.Result[Out]{Value: val, Err: err}}:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(raw)
	}()

	// reorder
	out := make(chan fresult.Result[Out], workers)
	go func() {
		defer close(out)
		pending := make(map[int]fresult.Result[Out])
		next := 0
		for r := range raw {
			pending[r.idx] = r.res
			for {
				res, ok := pending[next]
				if !ok {
					break
				}
				select {
				case out <- res:
				case <-ctx.Done():
					return
				}
				delete(pending, next)
				next++
				<-window
			}
		}
	}()

	return out
}
