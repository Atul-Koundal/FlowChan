// Package pool runs heterogeneous tasks concurrently using a fixed
// number of worker goroutines. Any struct that implements Task can
// be submitted to a WorkPool regardless of its concrete type.
package pool

import (
	"context"
	"fmt"
	"sync"
)

// Task is the interface any unit of work must satisfy to run in a
// WorkPool. Returning a non-nil error marks the task as failed.
// A panic inside Process is recovered and treated as an error.
type Task interface {
	Process() error
}

// WorkPool runs a fixed list of tasks concurrently across a fixed
// number of worker goroutines. Create one with NewWorkPool and
// start it with Run. A WorkPool can only be Run once.
type WorkPool struct {
	tasks       []Task
	concurrency int
	maxRetries  int
	tasksChan   chan Task
	errors      chan error
}

// Option configures a WorkPool. Pass Options to NewWorkPool.
type Option func(*WorkPool)

// WithRetries sets how many additional attempts a failing task gets
// before its error is recorded. A value of 3 means the task runs
// at most 4 times total (1 original + 3 retries).
func WithRetries(n int) Option {
	return func(wp *WorkPool) {
		wp.maxRetries = n
	}
}

// NewWorkPool creates a WorkPool that will run tasks using concurrency
// worker goroutines. Pass functional options to configure retries or
// other behaviour.
func NewWorkPool(tasks []Task, concurrency int, opts ...Option) *WorkPool {
	wp := &WorkPool{
		tasks:       tasks,
		concurrency: concurrency,
		maxRetries:  0,
		errors:      make(chan error, len(tasks)),
	}
	for _, o := range opts {
		o(wp)
	}
	return wp
}

// runWithRetry runs task.Process up to 1+maxRetries times, stopping
// on the first success. Returns the last error if all attempts fail.
func (wp *WorkPool) runWithRetry(task Task) error {
	var err error
	for attempt := 0; attempt <= wp.maxRetries; attempt++ {
		err = wp.safeProcess(task)
		if err == nil {
			return nil
		}
	}
	return err
}

// safeProcess calls task.Process and recovers any panic, converting
// it into an error so one misbehaving task cannot crash the pool.
func (wp *WorkPool) safeProcess(task Task) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in task: %v", r)
		}
	}()
	return task.Process()
}

// worker is the per-goroutine loop. Exits when tasksChan closes
// or the context is cancelled.
func (wp *WorkPool) worker(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case task, ok := <-wp.tasksChan:
			if !ok {
				return
			}
			if err := wp.runWithRetry(task); err != nil {
				wp.errors <- err
			}
		case <-ctx.Done():
			return
		}
	}
}

// Run starts all workers and blocks until every task has been attempted
// or the context is cancelled. Returns all errors collected during the
// run including recovered panics. Run must not be called more than once.
func (wp *WorkPool) Run(ctx context.Context) []error {
	wp.tasksChan = make(chan Task, len(wp.tasks))

	var wg sync.WaitGroup
	for i := 0; i < wp.concurrency; i++ {
		wg.Add(1)
		go wp.worker(ctx, &wg)
	}

	go func() {
		defer close(wp.tasksChan)
		for _, task := range wp.tasks {
			select {
			case wp.tasksChan <- task:
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()
	close(wp.errors)

	var errs []error
	for err := range wp.errors {
		errs = append(errs, err)
	}
	return errs
}