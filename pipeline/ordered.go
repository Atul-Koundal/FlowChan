package pipeline

import (
	"context"
	"sync"

	fresult "github.com/Atul-Koundal/FlowChan/result"
)

//Same as a stage but guarantees output order matches input order. 
// Without this, concurrent workers finish in unpredictable order.
// This file tags each item with a sequence number before processing, 
// then holds results in a buffer and only releases them in the correct sequence.

// indexed wraps a value with its sequence number.
type indexed[T any] struct {
	idx   int
	value T
}

// OrderedStage processes items concurrently but emits
// results in the exact same order as the input.
type OrderedStage[In, Out any] struct {
	workers int
	fn      func(context.Context, In) (Out, error)
}

func NewOrderedStage[In, Out any](workers int, fn func(context.Context, In) (Out, error)) *OrderedStage[In, Out] {
	return &OrderedStage[In, Out]{workers: workers, fn: fn}
}

func (s *OrderedStage[In, Out]) Run(ctx context.Context, in <-chan In) <-chan fresult.Result[Out] {
	// step 1: tag each input with a sequence number
	indexed_in := make(chan indexed[In], s.workers)
	go func() {
		defer close(indexed_in)
		i := 0
		for {
			select {
			case val, ok := <-in:
				if !ok {
					return
				}
				indexed_in <- indexed[In]{idx: i, value: val}
				i++
			case <-ctx.Done():
				return
			}
		}
	}()

	// step 2: process concurrently, emit tagged results
	type indexedResult struct {
		idx int
		res fresult.Result[Out]
	}

	rawOut := make(chan indexedResult, s.workers)
	var wg sync.WaitGroup
	for i := 0; i < s.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case item, ok := <-indexed_in:
					if !ok {
						return
					}
					val, err := s.fn(ctx, item.value)
					rawOut <- indexedResult{
						idx: item.idx,
						res: fresult.Result[Out]{Value: val, Err: err},
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(rawOut)
	}()

	// step 3: reorder by sequence number before emitting
	out := make(chan fresult.Result[Out], s.workers)
	go func() {
		defer close(out)
		pending := make(map[int]fresult.Result[Out])
		next := 0

		for r := range rawOut {
			pending[r.idx] = r.res
			// flush all consecutive results we now have
			for {
				res, ok := pending[next]
				if !ok {
					break
				}
				out <- res
				delete(pending, next)
				next++
			}
		}
	}()

	return out
}