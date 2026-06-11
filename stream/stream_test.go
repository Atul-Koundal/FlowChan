package stream

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"
	"sync/atomic"
)

func toChan[T any](items ...T) <-chan T {
	ch := make(chan T, len(items))
	for _, item := range items {
		ch <- item
	}
	close(ch)
	return ch
}

func TestMap_HappyPath(t *testing.T) {
	out := Map(context.Background(), toChan(1, 2, 3, 4, 5), 3,
		func(ctx context.Context, n int) (int, error) {
			return n * 2, nil
		})

	var results []int
	for r := range out {
		if r.IsErr() {
			t.Fatal("unexpected error:", r.Err)
		}
		results = append(results, r.Value)
	}

	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
}

func TestMap_ErrorPropagation(t *testing.T) {
	out := Map(context.Background(), toChan(1, 2, 3), 2,
		func(ctx context.Context, n int) (int, error) {
			if n == 2 {
				return 0, fmt.Errorf("bad item %d", n)
			}
			return n, nil
		})

	var errCount int
	for r := range out {
		if r.IsErr() {
			errCount++
		}
	}

	if errCount != 1 {
		t.Errorf("expected 1 error, got %d", errCount)
	}
}

func TestOrderedMap_PreservesOrder(t *testing.T) {
	out := OrderedMap(context.Background(), toChan(1, 2, 3, 4, 5), 3,
		func(ctx context.Context, n int) (int, error) {
			return n * 10, nil
		})

	var results []int
	for r := range out {
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

func TestFlatMap_Expansion(t *testing.T) {
	// each number expands into [n, n*2, n*3]
	out := FlatMap(context.Background(), toChan(1, 2, 3), 2,
		func(ctx context.Context, n int) ([]int, error) {
			return []int{n, n * 2, n * 3}, nil
		})

	var results []int
	for r := range out {
		if r.IsErr() {
			t.Fatal("unexpected error:", r.Err)
		}
		results = append(results, r.Value)
	}

	// 3 items × 3 expansions = 9 total
	if len(results) != 9 {
		t.Fatalf("expected 9 results, got %d", len(results))
	}
}

func TestFlatMap_ErrorPropagation(t *testing.T) {
	out := FlatMap(context.Background(), toChan(1, 2, 3), 2,
		func(ctx context.Context, n int) ([]int, error) {
			if n == 2 {
				return nil, fmt.Errorf("failed on %d", n)
			}
			return []int{n, n * 2}, nil
		})

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
	if valCount != 4 {
		t.Errorf("expected 4 values, got %d", valCount)
	}
}

func TestMap_Cancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	out := Map(ctx, toChan(1, 2, 3, 4, 5), 3,
		func(ctx context.Context, n int) (int, error) {
			return n, nil
		})

	// should not hang
	for range out {
	}
}

func TestFlatMap_EmptyExpansion(t *testing.T) {
	// fn returns empty slice for some items
	out := FlatMap(context.Background(), toChan(1, 2, 3), 2,
		func(ctx context.Context, n int) ([]int, error) {
			if n == 2 {
				return []int{}, nil // empty expansion
			}
			return []int{n}, nil
		})

	var results []int
	for r := range out {
		results = append(results, r.Value)
	}

	// only items 1 and 3 expanded, item 2 produced nothing
	sort.Ints(results)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

func TestBackpressureMap_LimitsInFlight(t *testing.T) {
	var active atomic.Int32
	var maxSeen atomic.Int32

	in := make(chan int, 20)
	go func() {
		defer close(in)
		for i := 0; i < 20; i++ {
			in <- i
		}
	}()

	out := BackpressureMap(context.Background(), in, 3,
		func(ctx context.Context, n int) (int, error) {
			current := active.Add(1)
			// track peak concurrency
			for {
				seen := maxSeen.Load()
				if current <= seen || maxSeen.CompareAndSwap(seen, current) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			active.Add(-1)
			return n * 2, nil
		})

	for range out {
	}

	if maxSeen.Load() > 3 {
		t.Errorf("backpressure failed: %d workers ran simultaneously, limit is 3", maxSeen.Load())
	}
}

func TestBackpressureMap_Cancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	in := make(chan int, 5)
	for i := 0; i < 5; i++ {
		in <- i
	}
	close(in)

	out := BackpressureMap(ctx, in, 3,
		func(ctx context.Context, n int) (int, error) {
			return n, nil
		})

	// should not hang
	for range out {
	}
}