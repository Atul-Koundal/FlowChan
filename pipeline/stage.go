// Package pipeline provides composable, concurrent processing stages.
// A Stage transforms a stream of inputs into a stream of Results,
// and Chain connects stages so the output of one feeds the next.
package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	ferrors "github.com/Atul-Koundal/FlowChan/errors"
)

// Stage represents a single concurrent transformation step.
// It reads from an input channel, applies fn using the configured
// number of workers, and writes results to an output channel.
type Stage[In, Out any] struct {
	workers   int
	fn        func(context.Context, In) (Out, error)
	rateLimit int // items per second, 0 means unlimited
	metrics   *Metrics
}

// StageOption configures optional behaviour on a Stage.
type StageOption[In, Out any] func(*Stage[In, Out])

// WithRateLimit caps the stage to itemsPerSecond across all workers
// combined. Use this to avoid overwhelming downstream APIs or databases.
func WithRateLimit[In, Out any](itemsPerSecond int) StageOption[In, Out] {
	return func(s *Stage[In, Out]) {
		s.rateLimit = itemsPerSecond
	}
}

// WithMetrics attaches a Metrics collector to the stage. Call Snapshot()
// on it at any time, from any goroutine, to inspect throughput, failures,
// and active workers while the stage is running.
func WithMetrics[In, Out any](m *Metrics) StageOption[In, Out] {
	return func(s *Stage[In, Out]) {
		s.metrics = m
	}
}

// NewStage creates a stage that runs fn across the given number of
// concurrent workers.
func NewStage[In, Out any](workers int, fn func(context.Context, In) (Out, error), opts ...StageOption[In, Out]) *Stage[In, Out] {
	s := &Stage[In, Out]{workers: workers, fn: fn}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Run starts the stage. It returns immediately with an output channel
// that closes once all input has been processed or the context is done.
// Panics inside fn are recovered and converted into Result errors.
func (s *Stage[In, Out]) Run(ctx context.Context, in <-chan In) <-chan ferrors.Result[Out] {
	out := make(chan ferrors.Result[Out], s.workers)

	var limiter *time.Ticker
	var limiterC <-chan time.Time
	if s.rateLimit > 0 {
		limiter = time.NewTicker(time.Second / time.Duration(s.rateLimit))
		limiterC = limiter.C
	}

	var wg sync.WaitGroup
	for i := 0; i < s.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.runWorker(ctx, in, out, limiterC)
		}()
	}

	go func() {
		wg.Wait()
		if limiter != nil {
			limiter.Stop()
		}
		close(out)
	}()

	return out
}

// runWorker is the per-goroutine loop. Separated from Run so that
// recover() cleanly catches a panic from a single item without
// taking down the whole stage.
func (s *Stage[In, Out]) runWorker(ctx context.Context, in <-chan In, out chan<- ferrors.Result[Out], limiterC <-chan time.Time) {
	for {
		select {
		case val, ok := <-in:
			if !ok {
				return
			}

			if limiterC != nil {
				select {
				case <-limiterC:
				case <-ctx.Done():
					return
				}
			}

			if s.metrics != nil {
				s.metrics.active.Add(1)
			}

			result, err := s.process(ctx, val)

			if s.metrics != nil {
				s.metrics.active.Add(-1)
				s.metrics.processed.Add(1)
				if err != nil {
					s.metrics.failed.Add(1)
				}
			}

			select {
			case out <- ferrors.Result[Out]{Value: result, Err: err}:
			case <-ctx.Done():
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

// process calls fn and recovers from any panic, converting it into
// an error so a single bad item cannot crash the whole pipeline.
func (s *Stage[In, Out]) process(ctx context.Context, val In) (out Out, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in stage: %v", r)
		}
	}()
	return s.fn(ctx, val)
}

// Pipeline chains multiple stages so the output of one feeds the next.
type Pipeline[In, Out any] struct {
	run func(context.Context, <-chan In) <-chan ferrors.Result[Out]
}

// Chain connects two stages into a single pipeline. Errors produced
// by the first stage are NOT dropped - they are forwarded directly
// to the final output as Result[Out]{Err: ...}, alongside results
// from the second stage. Only successful values from the first stage
// are passed into the second.
func Chain[In, Mid, Out any](
	first *Stage[In, Mid],
	second *Stage[Mid, Out],
) *Pipeline[In, Out] {
	return &Pipeline[In, Out]{
		run: func(ctx context.Context, in <-chan In) <-chan ferrors.Result[Out] {
			midResults := first.Run(ctx, in)

			midValues := make(chan Mid, first.workers)
			errOut := make(chan ferrors.Result[Out], first.workers)

			go func() {
				defer close(midValues)
				defer close(errOut)
				for r := range midResults {
					if r.Err != nil {
						select {
						case errOut <- ferrors.Result[Out]{Err: r.Err}:
						case <-ctx.Done():
							return
						}
						continue
					}
					select {
					case midValues <- r.Value:
					case <-ctx.Done():
						return
					}
				}
			}()

			secondOut := second.Run(ctx, midValues)

			finalOut := make(chan ferrors.Result[Out], second.workers)
			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()
				for r := range errOut {
					select {
					case finalOut <- r:
					case <-ctx.Done():
						return
					}
				}
			}()

			go func() {
				defer wg.Done()
				for r := range secondOut {
					select {
					case finalOut <- r:
					case <-ctx.Done():
						return
					}
				}
			}()

			go func() {
				wg.Wait()
				close(finalOut)
			}()

			return finalOut
		},
	}
}

// Run executes the pipeline against in and returns the combined
// output stream from all stages.
func (p *Pipeline[In, Out]) Run(ctx context.Context, in <-chan In) <-chan ferrors.Result[Out] {
	return p.run(ctx, in)
}