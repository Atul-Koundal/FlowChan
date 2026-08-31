// Package iter provides push iterators for consuming FlowChan streams
// without writing raw channel loops.
package iter

import (
	"context"

	fresult "github.com/Atul-Koundal/FlowChan/result"
)

// Seq is a push iterator that calls yield for each item. Returning
// false from yield stops iteration immediately.
type Seq[T any] func(yield func(T) bool)

// SeqErr is a push iterator that carries both a value and an error.
type SeqErr[T any] func(yield func(T, error) bool)

// FromChan converts a channel into a Seq iterator.
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
func FromResults[T any](ctx context.Context, ch <-chan fresult.Result[T]) SeqErr[T] {
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

// ToChan converts a Seq iterator back into a channel.
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

// Map transforms each item in seq using fn.
func Map[In, Out any](seq Seq[In], fn func(In) Out) Seq[Out] {
	return func(yield func(Out) bool) {
		seq(func(item In) bool {
			return yield(fn(item))
		})
	}
}

// Filter returns a Seq containing only items where fn returns true.
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

// Collect drains seq into a slice and returns it.
func Collect[T any](seq Seq[T]) []T {
	var items []T
	seq(func(item T) bool {
		items = append(items, item)
		return true
	})
	return items
}