package stream

import (
	"context"

	fresult "github.com/Atul-Koundal/FlowChan/result"
)

// Split divides a stream into two output channels based on predicate.
// Items where predicate returns true go to the first channel,
// items where it returns false go to the second. Each item goes
// to exactly one output. Errors, including a panic inside predicate,
// always go to the first channel. Both channels must be consumed.
func Split[T any](
	ctx context.Context,
	in <-chan fresult.Result[T],
	predicate func(T) bool,
) (<-chan fresult.Result[T], <-chan fresult.Result[T]) {
	matched := make(chan fresult.Result[T], 1)
	unmatched := make(chan fresult.Result[T], 1)

	send := func(ch chan<- fresult.Result[T], r fresult.Result[T]) bool {
		select {
		case ch <- r:
			return true
		case <-ctx.Done():
			return false
		}
	}

	go func() {
		defer close(matched)
		defer close(unmatched)

		for {
			select {
			case item, ok := <-in:
				if !ok {
					return
				}
				if item.IsErr() {
					if !send(matched, item) {
						return
					}
					continue
				}
				isMatch, perr := fresult.Catch(func() (bool, error) { return predicate(item.Value), nil })
				if perr != nil {
					if !send(matched, fresult.Result[T]{Err: perr}) {
						return
					}
					continue
				}
				target := unmatched
				if isMatch {
					target = matched
				}
				if !send(target, item) {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return matched, unmatched
}

// Merge combines multiple Result streams into one. It is an alias of
// FanIn kept for readability alongside Split.
func Merge[T any](ctx context.Context, inputs ...<-chan fresult.Result[T]) <-chan fresult.Result[T] {
	return FanIn(ctx, inputs...)
}
