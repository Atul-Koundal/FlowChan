package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	ferrors "github.com/Atul-Koundal/FlowChan/errors"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// helper - sends items into a channel and closes it
func toChan[T any](items ...T) <-chan T {
	ch := make(chan T, len(items))
	for _, item := range items {
		ch <- item
	}
	close(ch)
	return ch
}

// helper - collects all results from a channel into a slice
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
	cancel()

	stage := NewStage(3, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})

	out := stage.Run(ctx, toChan(1, 2, 3, 4, 5))
	collect(out)
}

func TestChain_TwoStages(t *testing.T) {
	stage1 := NewStage(2, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})

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

func TestChain_PropagatesStage1Errors(t *testing.T) {
	stage1 := NewStage(2, func(ctx context.Context, n int) (int, error) {
		if n == 3 {
			return 0, fmt.Errorf("stage1 failed on %d", n)
		}
		return n * 2, nil
	})

	stage2 := NewStage(2, func(ctx context.Context, n int) (string, error) {
		return fmt.Sprintf("val-%d", n), nil
	})

	p := Chain(stage1, stage2)
	out := p.Run(context.Background(), toChan(1, 2, 3, 4, 5))

	var errCount, valCount int
	for r := range out {
		if r.IsErr() {
			errCount++
		} else {
			valCount++
		}
	}

	if errCount != 1 {
		t.Errorf("expected 1 error from stage1, got %d", errCount)
	}
	if valCount != 4 {
		t.Errorf("expected 4 values, got %d", valCount)
	}
}

func TestStage_RecoversFromPanic(t *testing.T) {
	stage := NewStage(2, func(ctx context.Context, n int) (int, error) {
		if n == 3 {
			panic("boom")
		}
		return n * 2, nil
	})

	out := stage.Run(context.Background(), toChan(1, 2, 3, 4, 5))

	var errCount, valCount int
	for r := range out {
		if r.IsErr() {
			errCount++
		} else {
			valCount++
		}
	}

	if errCount != 1 {
		t.Errorf("expected 1 panic-converted error, got %d", errCount)
	}
	if valCount != 4 {
		t.Errorf("expected 4 successful values, got %d", valCount)
	}
}

func TestStage_RateLimit(t *testing.T) {
	stage := NewStage[int, int](5, func(ctx context.Context, n int) (int, error) {
		return n, nil
	}, WithRateLimit[int, int](10)) // 10 items/sec

	start := time.Now()
	out := stage.Run(context.Background(), toChan(1, 2, 3, 4, 5))
	for range out {
	}
	elapsed := time.Since(start)

	// 5 items at 10/sec should take roughly 400-500ms
	if elapsed < 300*time.Millisecond {
		t.Errorf("rate limit not enforced, took only %v", elapsed)
	}
}

func TestStage_Metrics(t *testing.T) {
	m := NewMetrics()
	stage := NewStage[int, int](3, func(ctx context.Context, n int) (int, error) {
		if n == 3 {
			return 0, fmt.Errorf("fail")
		}
		return n, nil
	}, WithMetrics[int, int](m))

	out := stage.Run(context.Background(), toChan(1, 2, 3, 4, 5))
	for range out {
	}

	snap := m.Snapshot()
	if snap.Processed != 5 {
		t.Errorf("expected 5 processed, got %d", snap.Processed)
	}
	if snap.Failed != 1 {
		t.Errorf("expected 1 failed, got %d", snap.Failed)
	}
	if snap.Active != 0 {
		t.Errorf("expected 0 active after completion, got %d", snap.Active)
	}
}

func TestOrderedStage_PreservesOrder(t *testing.T) {
	stage := NewOrderedStage(5, func(ctx context.Context, n int) (int, error) {
		time.Sleep(time.Duration(10-n) * time.Millisecond)
		return n * 10, nil
	})

	in := make(chan int, 5)
	go func() {
		defer close(in)
		for i := 1; i <= 5; i++ {
			in <- i
		}
	}()

	var results []int
	for r := range stage.Run(context.Background(), in) {
		if r.IsErr() {
			t.Fatal("unexpected error:", r.Err)
		}
		results = append(results, r.Value)
	}

	expected := []int{10, 20, 30, 40, 50}
	for i, v := range results {
		if v != expected[i] {
			t.Errorf("position %d: expected %d got %d", i, expected[i], v)
		}
	}
}