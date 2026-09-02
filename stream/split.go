package stream

import (
	"context"
	"sync"

	fresult "github.com/Atul-Koundal/FlowChan/result"
)

// Split divides a stream into two output channels based on predicate.
// Items where predicate returns true go to the first channel,
// items where it returns false go to the second. Each item goes
// to exactly one output. Errors always go to the first channel.
func Split[T any](
	ctx context.Context,
	in <-chan fresult.Result[T],
	predicate func(T) bool,
) (<-chan fresult.Result[T], <-chan fresult.Result[T]) {
	matched := make(chan fresult.Result[T], 1)
	unmatched := make(chan fresult.Result[T], 1)

	go func() {
		defer close(matched)
		defer close(unmatched)

		for {
			select {
			case item, ok := <-in:
				if !ok {
					return
				}
				// errors always go to matched channel
				if item.IsErr() {
					select {
					case matched <- item:
					case <-ctx.Done():
						return
					}
					continue
				}
				if predicate(item.Value) {
					select {
					case matched <- item:
					case <-ctx.Done():
						return
					}
				} else {
					select {
					case unmatched <- item:
					case <-ctx.Done():
						return
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return matched, unmatched
}

// Merge combines multiple Result streams into one. Items arrive in
// the output as they come from any input. The output closes when
// all inputs are closed or the context is cancelled.
func Merge[T any](ctx context.Context, inputs ...<-chan fresult.Result[T]) <-chan fresult.Result[T] {
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