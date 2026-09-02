package batch

import (
	"context"
	"testing"
	"time"
)

func TestBatch_ManualFlush(t *testing.T) {
	b := New[int](100, 10*time.Second) // large size and timeout

	in := make(chan int, 3)
	in <- 1
	in <- 2
	in <- 3

	out := b.Run(context.Background(), in)

	// trigger manual flush before size or timeout fires
	b.Flush()

	select {
	case r := <-out:
		if r.IsErr() {
			t.Fatal("unexpected error")
		}
		if len(r.Value) != 3 {
			t.Errorf("expected 3 items, got %d", len(r.Value))
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for manual flush")
	}
}