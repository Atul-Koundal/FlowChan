package iter

import (
	"context"

	ferrors "github.com/Atul-Koundal/FlowChan/errors"
)

// Seq is a push iterator — calls yield for each item until yield returns false.
type Seq[T any] func(yield func(T) bool)

// SeqErr is a push iterator that also carries errors.
type SeqErr[T any] func(yield func(T, error) bool)

// FromChan converts a channel into an iterator.
// Stops when channel closes or context cancels.
func FromChan[T any](ctx context.Context, ch <-chan T) Seq[T] {
	return func(yield func(T) bool) {
		for {
			select {
			case item, ok := <-ch:
				if !ok {
					return
				}
				if !yield(item) {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}
}

// FromResults converts a Result channel into a SeqErr iterator.
func FromResults[T any](ctx context.Context, ch <-chan ferrors.Result[T]) SeqErr[T] {
	return func(yield func(T, error) bool) {
		for {
			select {
			case r, ok := <-ch:
				if !ok {
					return
				}
				if !yield(r.Value, r.Err) {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}
}

// ToChan converts an iterator back into a channel.
func ToChan[T any](ctx context.Context, seq Seq[T]) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		seq(func(item T) bool {
			select {
			case out <- item:
				return true
			case <-ctx.Done():
				return false
			}
		})
	}()
	return out
}

// Map transforms each item in an iterator.
func Map[In, Out any](seq Seq[In], fn func(In) Out) Seq[Out] {
	return func(yield func(Out) bool) {
		seq(func(item In) bool {
			return yield(fn(item))
		})
	}
}

// Filter keeps only items where fn returns true.
func Filter[T any](seq Seq[T], fn func(T) bool) Seq[T] {
	return func(yield func(T) bool) {
		seq(func(item T) bool {
			if fn(item) {
				return yield(item)
			}
			return true
		})
	}
}

// Collect drains an iterator into a slice.
func Collect[T any](seq Seq[T]) []T {
	var items []T
	seq(func(item T) bool {
		items = append(items, item)
		return true
	})
	return items
}