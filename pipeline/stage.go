package pipeline

import (
	"context"
	"sync"

	ferrors "FlowChan/errors"
)

// Stage represents a single unit of work in the pipeline.
// It takes an input channel and returns an output channel.
type Stage[In, Out any] struct {
	workers int
	fn      func(context.Context, In) (Out, error)
}

// NewStage creates a new stage with the given worker count and transform function.
func NewStage[In, Out any](workers int, fn func(context.Context, In) (Out, error)) *Stage[In, Out] {
	return &Stage[In, Out]{
		workers: workers,
		fn:      fn,
	}
}

// Run starts the stage — reads from in, processes concurrently, writes to output channel.
func (s *Stage[In, Out]) Run(ctx context.Context, in <-chan In) <-chan ferrors.Result[Out] {
	out := make(chan ferrors.Result[Out], s.workers)

	var wg sync.WaitGroup
	for i := 0; i < s.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case val, ok := <-in:
					if !ok {
						return
					}
					result, err := s.fn(ctx, val)
					out <- ferrors.Result[Out]{Value: result, Err: err}
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

// Pipeline chains multiple stages together.
type Pipeline[In, Out any] struct {
	run func(context.Context, <-chan In) <-chan ferrors.Result[Out]
}

// Chain connects two stages — output of first becomes input of second.
func Chain[In, Mid, Out any](
	first *Stage[In, Mid],
	second *Stage[Mid, Out],
) *Pipeline[In, Out] {
	return &Pipeline[In, Out]{
		run: func(ctx context.Context, in <-chan In) <-chan ferrors.Result[Out] {
			// run first stage
			midResults := first.Run(ctx, in)

			// unwrap successful results into a plain channel for second stage
			midValues := make(chan Mid, first.workers)
			go func() {
				defer close(midValues)
				for r := range midResults {
					if r.Err != nil {
						continue // errors are dropped here — we will improve this later
					}
					midValues <- r.Value
				}
			}()

			// run second stage on unwrapped values
			return second.Run(ctx, midValues)
		},
	}
}

// Run executes the full pipeline.
func (p *Pipeline[In, Out]) Run(ctx context.Context, in <-chan In) <-chan ferrors.Result[Out] {
	return p.run(ctx, in)
}