package pipeline

import (
	"context"
	"fmt"
	"testing"

	ferrors "FlowChan/errors"
)

// helper — sends items into a channel and closes it
func toChan[T any](items ...T) <-chan T {
	ch := make(chan T, len(items))
	for _, item := range items {
		ch <- item
	}
	close(ch)
	return ch
}

// helper — collects all results from a channel into a slice
func collect[T any](ch <-chan ferrors.Result[T]) []ferrors.Result[T] {
	var results []ferrors.Result[T]
	for r := range ch {
		results = append(results, r)
	}
	return results
}

func TestStage_HappyPath(t *testing.T) {
	stage := NewStage(3, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})

	out := stage.Run(context.Background(), toChan(1, 2, 3, 4, 5))
	results := collect(out)

	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	for _, r := range results {
		if r.IsErr() {
			t.Errorf("unexpected error: %v", r.Err)
		}
	}
}

func TestStage_ErrorPropagation(t *testing.T) {
	stage := NewStage(2, func(ctx context.Context, n int) (int, error) {
		if n == 3 {
			return 0, fmt.Errorf("bad input: %d", n)
		}
		return n * 2, nil
	})

	out := stage.Run(context.Background(), toChan(1, 2, 3, 4, 5))
	results := collect(out)

	var errCount int
	for _, r := range results {
		if r.IsErr() {
			errCount++
		}
	}
	if errCount != 1 {
		t.Errorf("expected 1 error, got %d", errCount)
	}
}

func TestStage_Cancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	stage := NewStage(3, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})

	out := stage.Run(ctx, toChan(1, 2, 3, 4, 5))
	collect(out) // should not hang
}

func TestChain_TwoStages(t *testing.T) {
	// stage 1: multiply by 2
	stage1 := NewStage(2, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})

	// stage 2: convert to string
	stage2 := NewStage(2, func(ctx context.Context, n int) (string, error) {
		return fmt.Sprintf("value-%d", n), nil
	})

	pipeline := Chain(stage1, stage2)
	out := pipeline.Run(context.Background(), toChan(1, 2, 3))
	results := collect(out)

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	for _, r := range results {
		if r.IsErr() {
			t.Errorf("unexpected error: %v", r.Err)
		}
	}
}