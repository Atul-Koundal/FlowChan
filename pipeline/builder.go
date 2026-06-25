package pipeline

import (
	"context"
	"sync"

	ferrors "github.com/Atul-Koundal/FlowChan/errors"
)

// Builder constructs a multi-stage pipeline by chaining stages
// one at a time. Errors from any stage flow to the final output
// without being dropped at intermediate steps.
type Builder[In, Out any] struct {
	run func(context.Context, <-chan In) <-chan ferrors.Result[Out]
}

// NewBuilder starts a pipeline builder with a single initial stage.
func NewBuilder[In, Out any](stage *Stage[In, Out]) *Builder[In, Out] {
	return &Builder[In, Out]{
		run: func(ctx context.Context, in <-chan In) <-chan ferrors.Result[Out] {
			return stage.Run(ctx, in)
		},
	}
}

// Pipe appends a new stage to the pipeline. The output of the current
// last stage becomes the input of next. Errors from all previous stages
// are forwarded to the final output unchanged.
func Pipe[In, Mid, Out any](b *Builder[In, Mid], next *Stage[Mid, Out]) *Builder[In, Out] {
	return &Builder[In, Out]{
		run: func(ctx context.Context, in <-chan In) <-chan ferrors.Result[Out] {
			midResults := b.run(ctx, in)

			// split: errors bypass next stage, values flow into it
			midValues := make(chan Mid, next.workers)
			errPassthrough := make(chan ferrors.Result[Out], next.workers)

			go func() {
				defer close(midValues)
				defer close(errPassthrough)
				for r := range midResults {
					if r.Err != nil {
						select {
						case errPassthrough <- ferrors.Result[Out]{Err: r.Err}:
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

			nextOut := next.Run(ctx, midValues)

			// merge errors and next stage output
			return merge(ctx, errPassthrough, nextOut, next.workers)
		},
	}
}

// Run executes the built pipeline against in and returns the output stream.
func (b *Builder[In, Out]) Run(ctx context.Context, in <-chan In) <-chan ferrors.Result[Out] {
	return b.run(ctx, in)
}

// merge fans two result channels into one, closing when both are done.
func merge[T any](ctx context.Context, a, b <-chan ferrors.Result[T], bufSize int) <-chan ferrors.Result[T] {
	out := make(chan ferrors.Result[T], bufSize)
	var wg sync.WaitGroup
	wg.Add(2)

	forward := func(ch <-chan ferrors.Result[T]) {
		defer wg.Done()
		for r := range ch {
			select {
			case out <- r:
			case <-ctx.Done():
				return
			}
		}
	}

	go forward(a)
	go forward(b)

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}