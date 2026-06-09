package stream

import (
	"context"
	"sync"

	ferrors "FlowChan/errors"
)

// Map applies fn to every item concurrently, results may arrive out of order.
func Map[In, Out any](
	ctx context.Context,
	in <-chan In,
	workers int,
	fn func(context.Context, In) (Out, error),
) <-chan ferrors.Result[Out] {
	out := make(chan ferrors.Result[Out], workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case item, ok := <-in:
					if !ok {
						return
					}
					val, err := fn(ctx, item)
					out <- ferrors.Result[Out]{Value: val, Err: err}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// OrderedMap applies fn concurrently but preserves input order in output.
func OrderedMap[In, Out any](
	ctx context.Context,
	in <-chan In,
	workers int,
	fn func(context.Context, In) (Out, error),
) <-chan ferrors.Result[Out] {
	// wrap each item with its sequence number
	type indexed struct {
		idx  int
		item In
	}

	type indexedResult struct {
		idx int
		res ferrors.Result[Out]
	}

	// index the input
	indexed_in := make(chan indexed, workers)
	go func() {
		defer close(indexed_in)
		i := 0
		for {
			select {
			case item, ok := <-in:
				if !ok {
					return
				}
				indexed_in <- indexed{idx: i, item: item}
				i++
			case <-ctx.Done():
				return
			}
		}
	}()

	// process concurrently, emit indexed results
	rawOut := make(chan indexedResult, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case idxItem, ok := <-indexed_in:
					if !ok {
						return
					}
					val, err := fn(ctx, idxItem.item)
					rawOut <- indexedResult{
						idx: idxItem.idx,
						res: ferrors.Result[Out]{Value: val, Err: err},
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(rawOut)
	}()

	// reorder results by sequence number
	out := make(chan ferrors.Result[Out], workers)
	go func() {
		defer close(out)

		// buffer holds results that arrived early
		buffer := make(map[int]ferrors.Result[Out])
		next := 0

		for r := range rawOut {
			buffer[r.idx] = r.res
			// flush all consecutive results we have
			for {
				res, ok := buffer[next]
				if !ok {
					break
				}
				out <- res
				delete(buffer, next)
				next++
			}
		}
	}()

	return out
}

// FlatMap applies fn to each item, fn returns a slice — all items
// from all slices are emitted individually downstream.
func FlatMap[In, Out any](
	ctx context.Context,
	in <-chan In,
	workers int,
	fn func(context.Context, In) ([]Out, error),
) <-chan ferrors.Result[Out] {
	out := make(chan ferrors.Result[Out], workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case item, ok := <-in:
					if !ok {
						return
					}
					vals, err := fn(ctx, item)
					if err != nil {
						out <- ferrors.Result[Out]{Err: err}
						continue
					}
					// emit each expanded item individually
					for _, v := range vals {
						out <- ferrors.Result[Out]{Value: v}
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}