// Package stream provides concurrent stream transformation functions.
// All functions return a channel that closes when input is exhausted
// or the context is cancelled. A panic in a user callback is recovered
// and delivered as a *result.PanicError, and every goroutine exits when
// the context is cancelled even if the consumer stops reading.
package stream

import (
	"context"
	"sync"

	"github.com/Atul-Koundal/FlowChan/internal/ordered"
	fresult "github.com/Atul-Koundal/FlowChan/result"
)

// MapOption configures optional behaviour on Map.
type MapOption func(*mapConfig)

type mapConfig struct {
	metrics *Metrics
}

// WithMetrics attaches a Metrics collector to a Map call.
func WithMetrics(m *Metrics) MapOption {
	return func(c *mapConfig) {
		c.metrics = m
	}
}

// Map applies fn to each item concurrently using workers goroutines
// (values below 1 are treated as 1). Results may arrive in any order.
// Use OrderedMap when output order must match input order.
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
	if workers < 1 {
		workers = 1
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
					val, err := fresult.Catch(func() (Out, error) { return fn(ctx, item) })
					if cfg.metrics != nil {
						cfg.metrics.active.Add(-1)
						cfg.metrics.processed.Add(1)
						if err != nil {
							cfg.metrics.failed.Add(1)
						}
					}
					select {
					case out <- fresult.Result[Out]{Value: val, Err: err}:
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
		close(out)
	}()

	return out
}

// OrderedMap applies fn concurrently but guarantees output order
// matches input order. Workers may run at most a few times their count
// ahead of the oldest unfinished item, which bounds memory when one
// item is slow.
func OrderedMap[In, Out any](
	ctx context.Context,
	in <-chan In,
	workers int,
	fn func(context.Context, In) (Out, error),
) <-chan fresult.Result[Out] {
	return ordered.Run(ctx, in, workers, fn)
}

// FlatMap applies fn to each item where fn returns a slice. All items
// from all slices are emitted individually downstream.
func FlatMap[In, Out any](
	ctx context.Context,
	in <-chan In,
	workers int,
	fn func(context.Context, In) ([]Out, error),
) <-chan fresult.Result[Out] {
	if workers < 1 {
		workers = 1
	}
	out := make(chan fresult.Result[Out], workers)

	send := func(r fresult.Result[Out]) bool {
		select {
		case out <- r:
			return true
		case <-ctx.Done():
			return false
		}
	}

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
					vals, err := fresult.Catch(func() ([]Out, error) { return fn(ctx, item) })
					if err != nil {
						if !send(fresult.Result[Out]{Err: err}) {
							return
						}
						continue
					}
					for _, v := range vals {
						if !send(fresult.Result[Out]{Value: v}) {
							return
						}
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
