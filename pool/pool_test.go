package pool

import (
    "context"
    "errors"
    "sync/atomic"
    "testing"
    "time"
    "fmt"
)

// --- test task types ---

type successTask struct {
    processed atomic.Bool
}

func (t *successTask) Process() error {
    t.processed.Store(true)
    return nil
}

type slowTask struct{}

func (t *slowTask) Process() error {
    time.Sleep(500 * time.Millisecond)
    return nil
}

type failTask struct {
    msg string
}

func (t *failTask) Process() error {
    return errors.New(t.msg)
}

// --- tests ---

func TestAllTasksProcessed(t *testing.T) {
    tasks := []*successTask{{}, {}, {}, {}, {}}

    poolTasks := make([]Task, len(tasks))
    for i, tk := range tasks {
        poolTasks[i] = tk
    }

    wp := NewWorkPool(poolTasks, 3)
    errs := wp.Run(context.Background())

    if len(errs) != 0 {
        t.Fatalf("expected no errors, got %d", len(errs))
    }
    for i, tk := range tasks {
        if !tk.processed.Load() {
            t.Errorf("task %d was never processed", i)
        }
    }
}

func TestErrorsAreCollected(t *testing.T) {
    tasks := []Task{
        &failTask{msg: "db connection failed"},
        &failTask{msg: "timeout"},
        &successTask{},
    }

    wp := NewWorkPool(tasks, 2)
    errs := wp.Run(context.Background())

    if len(errs) != 2 {
        t.Fatalf("expected 2 errors, got %d", len(errs))
    }
}

func TestEmptyTaskList(t *testing.T) {
    wp := NewWorkPool([]Task{}, 3)
    errs := wp.Run(context.Background())

    if len(errs) != 0 {
        t.Fatalf("expected no errors, got %v", errs)
    }
}

func TestCancellation(t *testing.T) {
    tasks := make([]Task, 20)
    for i := range tasks {
        tasks[i] = &slowTask{}
    }

    ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
    defer cancel()

    wp := NewWorkPool(tasks, 2)
    wp.Run(ctx) // should return early, not hang
}

func TestConcurrencyLimit(t *testing.T) {
    var active atomic.Int32
    var maxSeen atomic.Int32

    type countingTask struct{}
    // we need a task that tracks concurrency, so use a closure-based approach
    tasks := make([]Task, 10)
    for i := range tasks {
        tasks[i] = &trackingTask{
            active:  &active,
            maxSeen: &maxSeen,
        }
    }

    wp := NewWorkPool(tasks, 3)
    wp.Run(context.Background())

    if maxSeen.Load() > 3 {
        t.Errorf("concurrency exceeded limit: saw %d active workers, limit is 3", maxSeen.Load())
    }
}

type trackingTask struct {
    active  *atomic.Int32
    maxSeen *atomic.Int32
}

func (t *trackingTask) Process() error {
    current := t.active.Add(1)
    for {
        seen := t.maxSeen.Load()
        if current <= seen || t.maxSeen.CompareAndSwap(seen, current) {
            break
        }
    }
    time.Sleep(50 * time.Millisecond)
    t.active.Add(-1)
    return nil
}

func TestRetries_EventualSuccess(t *testing.T) {
	attempts := 0
	// task fails first 2 times, succeeds on 3rd
	task := &funcTask{fn: func() error {
		attempts++
		if attempts < 3 {
			return fmt.Errorf("temporary failure")
		}
		return nil
	}}

	wp := NewWorkPool([]Task{task}, 1, WithRetries(3))
	errs := wp.Run(context.Background())

	if len(errs) != 0 {
		t.Fatalf("expected success after retries, got: %v", errs)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestRetries_ExhaustedReturnsError(t *testing.T) {
	task := &funcTask{fn: func() error {
		return fmt.Errorf("always fails")
	}}

	wp := NewWorkPool([]Task{task}, 1, WithRetries(2))
	errs := wp.Run(context.Background())

	if len(errs) != 1 {
		t.Fatalf("expected 1 error after retries exhausted, got %d", len(errs))
	}
}

// funcTask lets us pass a closure as a Task in tests
type funcTask struct {
	fn func() error
}

func (t *funcTask) Process() error {
	return t.fn()
}