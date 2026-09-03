package batch

import (
	"context"
	"testing"
	"time"
)

func TestBatch_ManualFlush(t *testing.T) {
	b := New[int](100, 10*time.Second) // large size and timeout

	in := make(chan int)
	out := b.Run(context.Background(), in)

	// send items after Run has started
	go func() {
		in <- 1
		in <- 2
		in <- 3
		// small delay to ensure items are buffered inside the batcher
		time.Sleep(20 * time.Millisecond)
		b.Flush()
	}()

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