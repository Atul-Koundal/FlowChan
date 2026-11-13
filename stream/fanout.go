package stream

import (
	"context"
	"sync"

	fresult "github.com/Atul-Koundal/FlowChan/result"
)

// FanOut distributes items from in across n output channels (n below 1
// is treated as 1). Each item goes to exactly one output: whichever
// consumer is ready first. A stalled consumer holds back at most the
// one item it is blocked on; the other outputs keep flowing. All
// outputs close when in closes or ctx is cancelled.
func FanOut[T any](ctx context.Context, in <-chan fresult.Result[T], n int) []<-chan fresult.Result[T] {
	if n < 1 {
		n = 1
	}
	outputs := make([]<-chan fresult.Result[T], n)
	for i := range outputs {
		out := make(chan fresult.Result[T], 1)
		outputs[i] = out
		go func() {
			defer close(out)
			for {
				select {
				case item, ok := <-in:
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
		}()
	}
	return outputs
}

// FanIn merges multiple input channels into a single output channel.
// Items from all inputs are forwarded as they arrive. The output
// channel closes when all input channels are closed or ctx is cancelled.
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
