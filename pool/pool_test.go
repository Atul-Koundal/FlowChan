package pool

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type funcTask struct {
	fn func() error
}

func (t *funcTask) Process() error { return t.fn() }

func TestPool_AllTasksProcessed(t *testing.T) {
	var count atomic.Int32
	wp := NewWorkPool(3)
	ctx := context.Background()
	wp.Start(ctx)

	for i := 0; i < 5; i++ {
		wp.Submit(&funcTask{fn: func() error {
			count.Add(1)
			return nil
		}})
	}

	wp.Drain()
	errs := wp.Stop()

	if count.Load() != 5 {
		t.Errorf("expected 5 processed, got %d", count.Load())
	}
	if len(errs) != 0 {
		t.Errorf("expected no errors, got %v", errs)
	}
}

func TestPool_ErrorsCollected(t *testing.T) {
	wp := NewWorkPool(2)
	wp.Start(context.Background())

	wp.Submit(&funcTask{fn: func() error { return fmt.Errorf("fail") }})
	wp.Submit(&funcTask{fn: func() error { return nil }})
	wp.Submit(&funcTask{fn: func() error { return fmt.Errorf("fail") }})

	wp.Drain()
	errs := wp.Stop()

	if len(errs) != 2 {
		t.Errorf("expected 2 errors, got %d", len(errs))
	}
}

func TestPool_Reusable(t *testing.T) {
	var count atomic.Int32
	wp := NewWorkPool(3)
	wp.Start(context.Background())

	// first batch
	for i := 0; i < 5; i++ {
		wp.Submit(&funcTask{fn: func() error {
			count.Add(1)
			return nil
		}})
	}
	wp.Drain()

	// second batch - same pool
	for i := 0; i < 5; i++ {
		wp.Submit(&funcTask{fn: func() error {
			count.Add(1)
			return nil
		}})
	}
	wp.Drain()
	wp.Stop()

	if count.Load() != 10 {
		t.Errorf("expected 10 total processed, got %d", count.Load())
	}
}

func TestPool_SubmitReturnsFalseAfterStop(t *testing.T) {
	wp := NewWorkPool(2)
	wp.Start(context.Background())
	wp.Stop()

	accepted := wp.Submit(&funcTask{fn: func() error { return nil }})
	if accepted {
		t.Error("expected Submit to return false after Stop")
	}
}

func TestPool_Retries(t *testing.T) {
	attempts := 0
	wp := NewWorkPool(1, WithRetries(2))
	wp.Start(context.Background())

	wp.Submit(&funcTask{fn: func() error {
		attempts++
		if attempts < 3 {
			return fmt.Errorf("not yet")
		}
		return nil
	}})

	wp.Drain()
	errs := wp.Stop()

	if len(errs) != 0 {
		t.Errorf("expected success after retries, got %v", errs)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestPool_Metrics(t *testing.T) {
	m := NewMetrics()
	wp := NewWorkPool(2, WithMetrics(m))
	wp.Start(context.Background())

	wp.Submit(&funcTask{fn: func() error { return nil }})
	wp.Submit(&funcTask{fn: func() error { return nil }})
	wp.Submit(&funcTask{fn: func() error { return fmt.Errorf("fail") }})

	wp.Drain()
	wp.Stop()

	snap := m.Snapshot()
	if snap.Processed != 3 {
		t.Errorf("expected 3 processed, got %d", snap.Processed)
	}
	if snap.Failed != 1 {
		t.Errorf("expected 1 failed, got %d", snap.Failed)
	}
}

func TestPool_RateLimit(t *testing.T) {
	wp := NewWorkPool(5, WithRateLimit(5)) // 5 tasks/sec
	wp.Start(context.Background())

	start := time.Now()
	for i := 0; i < 5; i++ {
		wp.Submit(&funcTask{fn: func() error { return nil }})
	}
	wp.Drain()
	elapsed := time.Since(start)
	wp.Stop()

	// 5 tasks at 5/sec should take at least 800ms
	if elapsed < 600*time.Millisecond {
		t.Errorf("rate limit not enforced, took only %v", elapsed)
	}
}