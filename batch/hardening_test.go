package batch

import (
	"context"
	"testing"
	"time"
)

// goleak (TestMain) fails the package if a batcher outlives its context.
func TestBatcher_CancelWithAbandonedConsumerLeavesNoGoroutine(t *testing.T) {
	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		in := make(chan int, 10)
		_ = New[int](1, time.Hour).Run(ctx, in) // consumer never reads
		for j := 0; j < 5; j++ {
			in <- j
		}
		time.Sleep(2 * time.Millisecond)
		cancel()
	}
}

func TestRealtimeBatcher_CancelWithAbandonedConsumerLeavesNoGoroutine(t *testing.T) {
	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		in := make(chan int, 10)
		_ = NewRealtime[int](1, time.Hour).Run(ctx, in)
		for j := 0; j < 5; j++ {
			in <- j
		}
		time.Sleep(2 * time.Millisecond)
		cancel()
	}
}

func TestBatcher_CancelDeliversPendingBatchToReadyConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan int, 3)
	out := New[int](10, time.Hour).Run(ctx, in)
	in <- 1
	in <- 2
	time.Sleep(20 * time.Millisecond)
	cancel()
	r, ok := <-out
	if !ok || len(r.Value) != 2 {
		t.Fatalf("want the pending batch of 2, got %+v ok=%v", r, ok)
	}
	if _, ok := <-out; ok {
		t.Fatal("channel should be closed")
	}
}

func TestBatcher_CancelWithNothingPendingEmitsNoEmptyBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	out := New[int](10, time.Hour).Run(ctx, make(chan int))
	cancel()
	for r := range out {
		t.Fatalf("unexpected batch %+v", r)
	}
}

func TestConstructors_InvalidArgsDoNotPanic(t *testing.T) {
	in := make(chan int, 1)
	in <- 1
	close(in)
	out := New[int](-1, 0).Run(context.Background(), in)
	if r := <-out; len(r.Value) != 1 {
		t.Fatalf("got %+v", r)
	}
	in2 := make(chan int, 1)
	in2 <- 1
	close(in2)
	out2 := NewRealtime[int](0, -time.Second).Run(context.Background(), in2)
	if r := <-out2; len(r.Value) != 1 {
		t.Fatalf("got %+v", r)
	}
}
