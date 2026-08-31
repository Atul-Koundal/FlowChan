// Package stream provides concurrent stream transformation functions.
// All functions return a channel that closes when input is exhausted
// or the context is cancelled.
package stream

import (
	"context"
	"sync"

	fresult "github.com/Atul-Koundal/FlowChan/result"
)

// MapOption configures optional behaviour on Map and OrderedMap.
type MapOption func(*mapConfig)

type mapConfig struct {
	metrics *Metrics
}

// WithMetrics attaches a Metrics collector to a Map or OrderedMap call.
func WithMetrics(m *Metrics) MapOption {
	return func(c *mapConfig) {
		c.metrics = m
	}
}


// Map applies fn to each item concurrently. Results may arrive in
// any order. Use OrderedMap when output order must match input order.
func Map[In, Out any](
	ctx context.Context,
	in <-chan In,
	workers int,
	fn func(context.Context, In) (Out, error),
	opts ...MapOption,
) <-chan fresult.Result[Out] {
	cfg := &mapConfig{}
	for _, o := range opts {
		o(cfg)
	}

	out := make(chan fresult.Result[Out], workers)
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
					if cfg.metrics != nil {
						cfg.metrics.active.Add(1)
					}
					val, err := fn(ctx, item)
					if cfg.metrics != nil {
						cfg.metrics.active.Add(-1)
						cfg.metrics.processed.Add(1)
						if err != nil {
							cfg.metrics.failed.Add(1)
						}
					}
					out <- fresult.Result[Out]{Value: val, Err: err}
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
// OrderedMap applies fn concurrently but guarantees output order
// matches input order. Slower than Map due to the reordering buffer.
func OrderedMap[In, Out any](
	ctx context.Context,
	in <-chan In,
	workers int,
	fn func(context.Context, In) (Out, error),
) <-chan fresult.Result[Out] {
	// wrap each item with its sequence number
	type indexed struct {
		idx  int
		item In
	}

	type indexedResult struct {
		idx int
		res fresult.Result[Out]
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
						res: fresult.Result[Out]{Value: val, Err: err},
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
	out := make(chan fresult.Result[Out], workers)
	go func() {
		defer close(out)

		// buffer holds results that arrived early
		buffer := make(map[int]fresult.Result[Out])
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

// FlatMap applies fn to each item where fn returns a slice. All items
// from all slices are emitted individually downstream.
func FlatMap[In, Out any](
	ctx context.Context,
	in <-chan In,
	workers int,
	fn func(context.Context, In) ([]Out, error),
) <-chan fresult.Result[Out] {
	out := make(chan fresult.Result[Out], workers)

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
						out <- fresult.Result[Out]{Err: err}
						continue
					}
					// emit each expanded item individually
					for _, v := range vals {
						out <- fresult.Result[Out]{Value: v}
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