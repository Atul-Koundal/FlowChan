package batch

import (
	"context"
	"testing"
	"time"
)

func TestRealtime_FlushesOnSilence(t *testing.T) {
	b := NewRealtime[int](100, 100*time.Millisecond)

	in := make(chan int, 3)
	in <- 1
	in <- 2
	in <- 3
	// don't send more - silence should trigger flush

	out := b.Run(context.Background(), in)

	select {
	case r := <-out:
		if r.IsErr() {
			t.Fatal("unexpected error")
		}
		if len(r.Value) != 3 {
			t.Errorf("expected 3 items, got %d", len(r.Value))
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out - sliding window did not flush")
	}
}

func TestRealtime_WindowResetsByNewItems(t *testing.T) {
	b := NewRealtime[int](100, 150*time.Millisecond)

	in := make(chan int)
	go func() {
		defer close(in)
		// send items every 100ms - window keeps resetting
		// so no flush should happen until we stop
		for i := 0; i < 3; i++ {
			in <- i
			time.Sleep(100 * time.Millisecond)
		}
		// now go silent - flush should happen after 150ms
	}()

	out := b.Run(context.Background(), in)

	var batches [][]int
	for r := range out {
		batches = append(batches, r.Value)
	}

	// all 3 items should arrive in one batch since channel closed
	if len(batches) != 1 {
		t.Errorf("expected 1 batch, got %d", len(batches))
	}
}

func TestRealtime_FlushesOnSizeLimit(t *testing.T) {
	b := NewRealtime[int](3, 1*time.Second)

	in := make(chan int, 6)
	for i := 1; i <= 6; i++ {
		in <- i
	}
	close(in)

	out := b.Run(context.Background(), in)

	var batches [][]int
	for r := range out {
		batches = append(batches, r.Value)
	}

	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(batches))
	}
}