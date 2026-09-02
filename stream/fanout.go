package stream

import (
	"context"
	"sync"

	fresult "github.com/Atul-Koundal/FlowChan/result"
)

// FanOut distributes items from in across n output channels.
// Each item goes to exactly one output channel  whichever worker
// is ready first. Use this to distribute work across independent
// consumers that each process a subset of items.
func FanOut[T any](ctx context.Context, in <-chan fresult.Result[T], n int) []<-chan fresult.Result[T] {
	outputs := make([]chan fresult.Result[T], n)
	for i := range outputs {
		outputs[i] = make(chan fresult.Result[T], 1)
	}

	go func() {
		// close all outputs when input is exhausted
		defer func() {
			for _, out := range outputs {
				close(out)
			}
		}()

		i := 0
		for {
			select {
			case item, ok := <-in:
				if !ok {
					return
				}
				// round robin across outputs
				select {
				case outputs[i%n] <- item:
				case <-ctx.Done():
					return
				}
				i++
			case <-ctx.Done():
				return
			}
		}
	}()

	// return as slice of receive-only channels
	result := make([]<-chan fresult.Result[T], n)
	for i, out := range outputs {
		result[i] = out
	}
	return result
}

// FanIn merges multiple input channels into a single output channel.
// Items from all inputs are forwarded as they arrive. The output
// channel closes when all input channels are closed.
func FanIn[T any](ctx context.Context, inputs ...<-chan fresult.Result[T]) <-chan fresult.Result[T] {
	out := make(chan fresult.Result[T], len(inputs))

	var wg sync.WaitGroup
	for _, in := range inputs {
		wg.Add(1)
		go func(ch <-chan fresult.Result[T]) {
			defer wg.Done()
			for {
				select {
				case item, ok := <-ch:
					if !ok {
						return
					}
					select {
					case out <- item:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}(in)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}