// Package pool runs heterogeneous tasks concurrently using a fixed
// number of worker goroutines. Any struct that implements Task can
// be submitted to a WorkPool regardless of its concrete type.
package pool

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Task is the interface any unit of work must satisfy to run in a
// WorkPool. Returning a non-nil error marks the task as failed.
// A panic inside Process is recovered and treated as an error.
type Task interface {
	Process() error
}

// WorkPool is a reusable pool of workers. Unlike the previous design,
// a WorkPool can accept work continuously via Submit. Call Start once,
// Submit tasks as they arrive, Drain to wait for the queue to empty,
// and Stop to shut down workers cleanly.
type WorkPool struct {
	concurrency int
	maxRetries  int
	rateLimit   int
	metrics     *Metrics
	tasksChan   chan Task
	errors      chan error
	wg          sync.WaitGroup
	once        sync.Once
	stopCh      chan struct{}
	mu          sync.Mutex
	errs        []error
}

// Option configures a WorkPool.
type Option func(*WorkPool)

// WithRetries sets how many additional attempts a failing task gets
// before its error is recorded.
func WithRetries(n int) Option {
	return func(wp *WorkPool) { wp.maxRetries = n }
}

// WithRateLimit caps the pool to itemsPerSecond tasks across all
// workers combined.
func WithRateLimit(itemsPerSecond int) Option {
	return func(wp *WorkPool) { wp.rateLimit = itemsPerSecond }
}

// WithMetrics attaches a Metrics collector to the pool.
func WithMetrics(m *Metrics) Option {
	return func(wp *WorkPool) { wp.metrics = m }
}

// NewWorkPool creates a WorkPool with the given concurrency.
// Call Start to launch workers, then Submit tasks as they arrive.
func NewWorkPool(concurrency int, opts ...Option) *WorkPool {
	wp := &WorkPool{
		concurrency: concurrency,
		tasksChan:   make(chan Task, concurrency*2),
		errors:      make(chan error, 1024),
		stopCh:      make(chan struct{}),
	}
	for _, o := range opts {
		o(wp)
	}
	return wp
}

// Start launches the worker goroutines. Must be called before Submit.
// Safe to call only once — subsequent calls are no-ops.
func (wp *WorkPool) Start(ctx context.Context) {
	wp.once.Do(func() {
		var limiterC <-chan time.Time
		if wp.rateLimit > 0 {
			ticker := time.NewTicker(time.Second / time.Duration(wp.rateLimit))
			limiterC = ticker.C
			go func() {
				<-wp.stopCh
				ticker.Stop()
			}()
		}

		for i := 0; i < wp.concurrency; i++ {
			wp.wg.Add(1)
			go wp.worker(ctx, limiterC)
		}

		// collect errors from workers
		go func() {
			for err := range wp.errors {
				wp.mu.Lock()
				wp.errs = append(wp.errs, err)
				wp.mu.Unlock()
			}
		}()
	})
}

// Submit sends a task to the pool for processing. Blocks if all
// workers are busy. Returns false if the pool has been stopped.
func (wp *WorkPool) Submit(task Task) bool {
	select {
	case <-wp.stopCh:
		return false
	default:
	}

	defer func() {
		recover() // catch send on closed channel race
	}()

	select {
	case wp.tasksChan <- task:
		return true
	case <-wp.stopCh:
		return false
	}
}

// Drain waits for all currently submitted tasks to finish.
// New tasks can still be submitted after Drain returns.
func (wp *WorkPool) Drain() {
	// send sentinel tasks equal to worker count to ensure
	// all workers have finished their current task
	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < wp.concurrency; i++ {
			wg.Add(1)
			wp.tasksChan <- &sentinelTask{wg: &wg}
		}
		wg.Wait()
		close(done)
	}()
	<-done
}

// Stop signals workers to exit and waits for all of them to finish.
// After Stop, Submit will return false. Errors collected during the
// run are returned.
func (wp *WorkPool) Stop() []error {
	close(wp.stopCh)
	close(wp.tasksChan)
	wp.wg.Wait()
	close(wp.errors)

	wp.mu.Lock()
	defer wp.mu.Unlock()
	return wp.errs
}

// worker is the per-goroutine loop.
func (wp *WorkPool) worker(ctx context.Context, limiterC <-chan time.Time) {
	defer wp.wg.Done()
	for {
		select {
		case task, ok := <-wp.tasksChan:
			if !ok {
				return
			}
			// skip sentinel tasks - they are only used by Drain
			if s, ok := task.(*sentinelTask); ok {
				s.wg.Done()
				continue
			}
			if limiterC != nil {
				select {
				case <-limiterC:
				case <-ctx.Done():
					return
				}
			}
			if wp.metrics != nil {
				wp.metrics.active.Add(1)
			}
			err := wp.runWithRetry(task)
			if wp.metrics != nil {
				wp.metrics.active.Add(-1)
				wp.metrics.processed.Add(1)
				if err != nil {
					wp.metrics.failed.Add(1)
				}
			}
			if err != nil {
				select {
				case wp.errors <- err:
				default:
					// error buffer full - store directly
					wp.mu.Lock()
					wp.errs = append(wp.errs, err)
					wp.mu.Unlock()
				}
			}
		case <-ctx.Done():
			return
		case <-wp.stopCh:
			return
		}
	}
}

// sentinelTask is used internally by Drain to detect when all
// workers have finished their current task.
type sentinelTask struct {
	wg *sync.WaitGroup
}

func (t *sentinelTask) Process() error {
	t.wg.Done()
	return nil
}

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

func (wp *WorkPool) safeProcess(task Task) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in task: %v", r)
		}
	}()
	return task.Process()
}