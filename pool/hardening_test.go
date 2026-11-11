package pool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Atul-Koundal/FlowChan/result"
)

// valueTask uses a value receiver; the pool used to crash the whole
// process on these via reflect.Type.Elem.
type valueTask struct{ err error }

func (v valueTask) Process() error { return v.err }

func TestPool_ValueReceiverTaskDoesNotCrash(t *testing.T) {
	wp := NewWorkPool(2)
	wp.Start(context.Background())
	wp.Submit(valueTask{err: errors.New("boom")})
	wp.Drain()
	errs := wp.Stop()
	var pe *result.PoolError
	if len(errs) != 1 || !errors.As(errs[0], &pe) || pe.TaskType != "valueTask" {
		t.Fatalf("unexpected errors: %v", errs)
	}
}

func TestPool_StopIsIdempotent(t *testing.T) {
	wp := NewWorkPool(2)
	wp.Start(context.Background())
	wp.Submit(&funcTask{fn: func() error { return errors.New("x") }})
	first := wp.Stop()
	second := wp.Stop() // used to panic: close of closed channel
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("first=%v second=%v", first, second)
	}
}

func TestPool_DrainWaitsForInFlightTasks(t *testing.T) {
	var done atomic.Bool
	wp := NewWorkPool(2)
	wp.Start(context.Background())
	started := make(chan struct{})
	wp.Submit(&funcTask{fn: func() error {
		close(started)
		time.Sleep(150 * time.Millisecond)
		done.Store(true)
		return nil
	}})
	<-started // one worker is busy, the other is idle
	wp.Drain()
	if !done.Load() {
		t.Fatal("Drain returned while a task was still running")
	}
	wp.Stop()
}

func TestPool_StopRunsEverythingAccepted(t *testing.T) {
	var ran atomic.Int32
	wp := NewWorkPool(1)
	wp.Start(context.Background())
	accepted := 0
	for i := 0; i < 3; i++ {
		if wp.Submit(&funcTask{fn: func() error { time.Sleep(30 * time.Millisecond); ran.Add(1); return nil }}) {
			accepted++
		}
	}
	wp.Stop() // no Drain: queued tasks must still run
	if int(ran.Load()) != accepted {
		t.Fatalf("accepted %d tasks but only %d ran", accepted, ran.Load())
	}
}

func TestPool_StopReturnsEveryError(t *testing.T) {
	for i := 0; i < 100; i++ {
		wp := NewWorkPool(8)
		wp.Start(context.Background())
		for j := 0; j < 200; j++ {
			wp.Submit(&funcTask{fn: func() error { return errors.New("x") }})
		}
		wp.Drain()
		if got := len(wp.Stop()); got != 200 {
			t.Fatalf("run %d: Stop returned %d errors, want 200", i, got)
		}
	}
}

func TestPool_CancelledTasksAreReportedNotDropped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wp := NewWorkPool(1)
	wp.Start(ctx)
	release := make(chan struct{})
	started := make(chan struct{})
	wp.Submit(&funcTask{fn: func() error { close(started); <-release; return nil }})
	<-started
	wp.Submit(&funcTask{fn: func() error { return nil }}) // queued behind the blocker
	cancel()
	close(release)
	errs := wp.Stop()
	if len(errs) != 1 || !errors.Is(errs[0], context.Canceled) {
		t.Fatalf("want 1 cancellation error, got %v", errs)
	}
}

func TestPool_SubmitAfterCancelReturnsFalse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wp := NewWorkPool(1)
	wp.Start(ctx)
	cancel()
	if wp.Submit(&funcTask{fn: func() error { return nil }}) {
		t.Fatal("Submit should refuse work after cancellation")
	}
	wp.Stop()
}

func TestPool_ZeroAndNegativeConcurrencyAreClamped(t *testing.T) {
	for _, n := range []int{0, -5} {
		var ran atomic.Int32
		wp := NewWorkPool(n)
		wp.Start(context.Background())
		wp.Submit(&funcTask{fn: func() error { ran.Add(1); return nil }})
		wp.Drain()
		wp.Stop()
		if ran.Load() != 1 {
			t.Fatalf("concurrency %d: task did not run", n)
		}
	}
}

func TestPool_SubmitBeforeStartOrNilReturnsFalse(t *testing.T) {
	wp := NewWorkPool(1)
	if wp.Submit(&funcTask{fn: func() error { return nil }}) {
		t.Fatal("Submit before Start must return false")
	}
	wp.Start(context.Background())
	if wp.Submit(nil) {
		t.Fatal("nil task must be rejected")
	}
	wp.Stop()
}

func TestPool_ConcurrentSubmitAndStopNeverPanics(t *testing.T) {
	for i := 0; i < 50; i++ {
		wp := NewWorkPool(4)
		wp.Start(context.Background())
		done := make(chan struct{})
		for g := 0; g < 4; g++ {
			go func() {
				defer func() { done <- struct{}{} }()
				for j := 0; j < 100; j++ {
					wp.Submit(&funcTask{fn: func() error { return nil }})
				}
			}()
		}
		wp.Stop()
		for g := 0; g < 4; g++ {
			<-done
		}
	}
}

func TestPool_PanicIsPanicErrorAndNotRetried(t *testing.T) {
	var calls atomic.Int32
	wp := NewWorkPool(1, WithRetries(3))
	wp.Start(context.Background())
	wp.Submit(&funcTask{fn: func() error { calls.Add(1); panic("bug") }})
	errs := wp.Stop()
	var pe *result.PanicError
	if len(errs) != 1 || !errors.As(errs[0], &pe) || calls.Load() != 1 {
		t.Fatalf("calls=%d errs=%v", calls.Load(), errs)
	}
}

func TestPool_RetryBackoffAndErrorHandler(t *testing.T) {
	var attempts atomic.Int32
	var handled atomic.Int32
	wp := NewWorkPool(1,
		WithRetries(2),
		WithRetryBackoff(func(int) time.Duration { return 10 * time.Millisecond }),
		WithErrorHandler(func(error) { handled.Add(1) }),
	)
	wp.Start(context.Background())
	start := time.Now()
	wp.Submit(&funcTask{fn: func() error { attempts.Add(1); return errors.New("nope") }})
	errs := wp.Stop()
	if attempts.Load() != 3 || handled.Load() != 1 || len(errs) != 0 {
		t.Fatalf("attempts=%d handled=%d retained=%d", attempts.Load(), handled.Load(), len(errs))
	}
	if time.Since(start) < 20*time.Millisecond {
		t.Fatal("backoff between retries was not applied")
	}
}
