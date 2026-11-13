package pipeline

import (
	"context"

	"github.com/Atul-Koundal/FlowChan/internal/ordered"
	fresult "github.com/Atul-Koundal/FlowChan/result"
)

// OrderedStage processes items concurrently but emits results in the
// exact same order as the input, regardless of how long each item takes.
// Workers may run only a bounded distance ahead of the oldest unfinished
// item, so one slow item cannot make memory grow without limit.
type OrderedStage[In, Out any] struct {
	workers int
	fn      func(context.Context, In) (Out, error)
}

// NewOrderedStage creates an order-preserving stage that runs fn across
// the given number of workers (values below 1 are treated as 1).
func NewOrderedStage[In, Out any](workers int, fn func(context.Context, In) (Out, error)) *OrderedStage[In, Out] {
	if workers < 1 {
		workers = 1
	}
	return &OrderedStage[In, Out]{workers: workers, fn: fn}
}

// Run starts the stage and returns its output channel, which closes
// once all input has been processed or ctx is done. Panics in fn are
// recovered as *result.PanicError.
func (s *OrderedStage[In, Out]) Run(ctx context.Context, in <-chan In) <-chan fresult.Result[Out] {
	return ordered.Run(ctx, in, s.workers, s.fn)
}
