package batch

import (
	"context"
	"testing"
	"time"
)

func TestBatch_ManualFlush(t *testing.T) {
	b := New[int](100, 10*time.Second)

	in := make(chan int)
	out := b.Run(context.Background(), in)

	go func() {
		in <- 1
		in <- 2
		in <- 3
		time.Sleep(20 * time.Millisecond)
		b.Flush()
		close(in) // close so batcher goroutine exits cleanly
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

	// drain out so the batcher goroutine can exit
	for range out {
	}
}