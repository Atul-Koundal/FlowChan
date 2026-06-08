package batch

import (
	"context"
	"testing"
	"time"
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

func TestBatch_BySize(t *testing.T) {
	b := New[int](3, 1*time.Second)
	out := b.Run(context.Background(), toChan(1, 2, 3, 4, 5, 6))

	var batches [][]int
	for r := range out {
		if r.IsErr() {
			t.Fatal("unexpected error")
		}
		batches = append(batches, r.Value)
	}

	// 6 items with size 3 = exactly 2 batches
	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(batches))
	}
	for _, b := range batches {
		if len(b) != 3 {
			t.Errorf("expected batch size 3, got %d", len(b))
		}
	}
}

func TestBatch_ByTimeout(t *testing.T) {
	b := New[int](100, 100*time.Millisecond)

	ch := make(chan int, 10)
	ch <- 1
	ch <- 2
	// don't close — let timeout flush it

	out := b.Run(context.Background(), ch)

	select {
	case r := <-out:
		if r.IsErr() {
			t.Fatal("unexpected error")
		}
		if len(r.Value) != 2 {
			t.Errorf("expected 2 items, got %d", len(r.Value))
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for batch flush")
	}
}

func TestBatch_PartialFinalBatch(t *testing.T) {
	// 5 items with size 3 — first batch has 3, second has 2
	b := New[int](3, 1*time.Second)
	out := b.Run(context.Background(), toChan(1, 2, 3, 4, 5))

	var batches [][]int
	for r := range out {
		batches = append(batches, r.Value)
	}

	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(batches))
	}
	if len(batches[1]) != 2 {
		t.Errorf("expected final batch size 2, got %d", len(batches[1]))
	}
}

func TestBatch_EmptyInput(t *testing.T) {
	b := New[int](3, 100*time.Millisecond)
	out := b.Run(context.Background(), toChan[int]())

	var batches [][]int
	for r := range out {
		batches = append(batches, r.Value)
	}

	if len(batches) != 0 {
		t.Errorf("expected no batches, got %d", len(batches))
	}
}

func TestBatch_Cancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	ch := make(chan int)
	b := New[int](10, 1*time.Second)
	out := b.Run(ctx, ch)

	cancel() // cancel immediately

	select {
	case <-out: // either a flush or close, both are fine
	case <-time.After(1 * time.Second):
		t.Fatal("timed out — cancellation did not unblock batcher")
	}
}