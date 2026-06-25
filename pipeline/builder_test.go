package pipeline

import (
	"context"
	"fmt"
	"testing"
)

func TestBuilder_ThreeStages(t *testing.T) {
	stage1 := NewStage(2, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})
	stage2 := NewStage(2, func(ctx context.Context, n int) (int, error) {
		return n + 1, nil
	})
	stage3 := NewStage(2, func(ctx context.Context, n int) (string, error) {
		return fmt.Sprintf("result-%d", n), nil
	})

	b := Pipe(Pipe(NewBuilder(stage1), stage2), stage3)
	out := b.Run(context.Background(), toChan(1, 2, 3))

	var results []string
	for r := range out {
		if r.IsErr() {
			t.Fatal("unexpected error:", r.Err)
		}
		results = append(results, r.Value)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
}

func TestBuilder_ErrorsPropagateThrough(t *testing.T) {
	stage1 := NewStage(2, func(ctx context.Context, n int) (int, error) {
		if n == 2 {
			return 0, fmt.Errorf("stage1 failed on %d", n)
		}
		return n * 2, nil
	})
	stage2 := NewStage(2, func(ctx context.Context, n int) (int, error) {
		return n + 10, nil
	})
	stage3 := NewStage(2, func(ctx context.Context, n int) (string, error) {
		return fmt.Sprintf("v-%d", n), nil
	})

	b := Pipe(Pipe(NewBuilder(stage1), stage2), stage3)
	out := b.Run(context.Background(), toChan(1, 2, 3))

	var errCount, valCount int
	for r := range out {
		if r.IsErr() {
			errCount++
		} else {
			valCount++
		}
	}

	if errCount != 1 {
		t.Errorf("expected 1 error, got %d", errCount)
	}
	if valCount != 2 {
		t.Errorf("expected 2 values, got %d", valCount)
	}
}