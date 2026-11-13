package pipeline

import (
	"context"
	"errors"
	"testing"

	fresult "github.com/Atul-Koundal/FlowChan/result"
)

func ints(n int) <-chan int {
	ch := make(chan int, n)
	for i := 0; i < n; i++ {
		ch <- i
	}
	close(ch)
	return ch
}

func TestStage_PanicInNamedStageIsWrappedWithStageName(t *testing.T) {
	st := NewStage(1, func(ctx context.Context, n int) (int, error) { panic("kaboom") },
		WithName[int, int]("parse"))
	for r := range st.Run(context.Background(), ints(1)) {
		var se *fresult.StageError
		var pe *fresult.PanicError
		if !errors.As(r.Err, &se) || se.Stage != "parse" || !errors.As(r.Err, &pe) {
			t.Fatalf("want StageError(parse) wrapping PanicError, got %v", r.Err)
		}
	}
}

func TestOrderedStage_PanicDoesNotCrashAndOrderHolds(t *testing.T) {
	st := NewOrderedStage(3, func(ctx context.Context, n int) (int, error) {
		if n == 3 {
			panic("x")
		}
		return n, nil
	})
	i := 0
	for r := range st.Run(context.Background(), ints(10)) {
		var pe *fresult.PanicError
		if i == 3 && !errors.As(r.Err, &pe) {
			t.Fatalf("want panic at 3, got %+v", r)
		}
		if i != 3 && (r.Err != nil || r.Value != i) {
			t.Fatalf("position %d: %+v", i, r)
		}
		i++
	}
	if i != 10 {
		t.Fatalf("got %d results", i)
	}
}

func TestOrderedStage_CancelWithAbandonedConsumerLeavesNoGoroutines(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan int)
	go func() {
		defer close(in)
		for i := 0; ; i++ {
			select {
			case in <- i:
			case <-ctx.Done():
				return
			}
		}
	}()
	_ = NewOrderedStage(3, func(ctx context.Context, n int) (int, error) { return n, nil }).Run(ctx, in)
	cancel()
}

func TestNewStage_NonPositiveWorkersStillProcesses(t *testing.T) {
	st := NewStage(0, func(ctx context.Context, n int) (int, error) { return n, nil })
	got := 0
	for range st.Run(context.Background(), ints(4)) {
		got++
	}
	if got != 4 {
		t.Fatalf("got %d", got)
	}
}
