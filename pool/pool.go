// Package pool runs heterogeneous tasks concurrently using a fixed
// number of worker goroutines. Any struct that implements Task can
// be submitted to a WorkPool regardless of its concrete type.
package pool

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/Atul-Koundal/FlowChan/result"
)

// Task is the interface any unit of work must satisfy to run in a
// WorkPool. Returning a non-nil error marks the task as failed.
// A panic inside Process is recovered and reported as a
// *result.PanicError; panicking tasks are never retried.
type Task interface {
	Process() error
}

// WorkPool is a reusable pool of workers. Call Start once, Submit
// tasks as they arrive, Drain to wait for everything accepted so far
// to finish, and Stop to shut the pool down.
//
// Every task accepted by Submit is guaranteed to end up either run or
// reported as an error: nothing is silently dropped.
type WorkPool struct {
	concurrency int
	maxRetries  int
	rateLimit   int
	backoff     func(attempt int) time.Duration
	onError     func(error)
	metrics     *Metrics

	tasks    chan Task
	stopCh   chan struct{}
	wg       sync.WaitGroup // worker goroutines
	startOne sync.Once
	stopOne  sync.Once

	mu       sync.Mutex
	cond     *sync.Cond // signalled when inflight reaches zero
	ctx      context.Context
	started  bool
	closed   bool
	inflight int // accepted tasks that have not finished yet
	errs     []error
	limiter  *time.Ticker
}

// Option configures a WorkPool.
type Option func(*WorkPool)

// WithRetries sets how many additional attempts a failing task gets
// before its error is recorded. A value of 3 means the task runs
// at most 4 times total (1 original + 3 retries).
func WithRetries(n int) Option {
	return func(wp *WorkPool) { wp.maxRetries = n }
}

// WithRetryBackoff sets how long to wait between retry attempts. The
// argument is the attempt number that just failed, starting at 0, so
// any retry.BackoffStrategy can be passed directly. Without it retries
// happen immediately.
func WithRetryBackoff(strategy func(attempt int) time.Duration) Option {
	return func(wp *WorkPool) { wp.backoff = strategy }
}

// WithRateLimit caps the pool to itemsPerSecond tasks across all
// workers combined. Use this when tasks call external services
// that enforce rate limits.
func WithRateLimit(itemsPerSecond int) Option {
	return func(wp *WorkPool) { wp.rateLimit = itemsPerSecond }
}

// WithMetrics attaches a Metrics collector to the pool. Call Snapshot()
// at any time to inspect throughput, failures, and active workers.
func WithMetrics(m *Metrics) Option {
	return func(wp *WorkPool) { wp.metrics = m }
}

// WithErrorHandler delivers each task failure to fn as it happens
// instead of collecting it for Stop. Use it for long-running pools,
// where retaining every error would grow without bound. fn may be
// called from several workers at once; a panic inside fn is swallowed.
func WithErrorHandler(fn func(error)) Option {
	return func(wp *WorkPool) { wp.onError = fn }
}

// NewWorkPool creates a WorkPool with the given concurrency (values
// below 1 are treated as 1). Call Start to launch workers, then Submit
// tasks as they arrive.
func NewWorkPool(concurrency int, opts ...Option) *WorkPool {
	if concurrency < 1 {
		concurrency = 1
	}
	wp := &WorkPool{
		concurrency: concurrency,
		tasks:       make(chan Task, concurrency*2),
		stopCh:      make(chan struct{}),
	}
	wp.cond = sync.NewCond(&wp.mu)
	for _, o := range opts {
		o(wp)
	}
	return wp
}

// Start launches the worker goroutines. It must be called before
// Submit; subsequent calls are no-ops. Cancelling ctx makes the pool
// stop running tasks: tasks that were accepted but had not started are
// reported as errors wrapping ctx.Err(), and Submit returns false.
// Stop must still be called to release the pool's goroutines.
func (wp *WorkPool) Start(ctx context.Context) {
	wp.startOne.Do(func() {
		if ctx == nil {
			ctx = context.Background()
		}
		wp.mu.Lock()
		wp.ctx = ctx
		wp.started = true
		if wp.rateLimit > 0 {
			interval := time.Second / time.Duration(wp.rateLimit)
			if interval <= 0 {
				interval = time.Nanosecond
			}
			wp.limiter = time.NewTicker(interval)
		}
		wp.mu.Unlock()

		for i := 0; i < wp.concurrency; i++ {
			wp.wg.Add(1)
			go wp.worker()
		}
	})
}

// Submit hands a task to the pool. It blocks while the queue is full
// or the rate limit is active, and returns false (without running the
// task) if the pool was not started, has been stopped, or its context
// is cancelled.
func (wp *WorkPool) Submit(task Task) bool {
	if task == nil {
		return false
	}

	wp.mu.Lock()
	if !wp.started || wp.closed || wp.ctx.Err() != nil {
		wp.mu.Unlock()
		return false
	}
	limiter, ctx := wp.limiter, wp.ctx
	wp.mu.Unlock()

	if limiter != nil {
		select {
		case <-limiter.C:
		case <-ctx.Done():
			return false
		}
	}

	// Re-check and count the task in one critical section so Stop can
	// never miss a task that is about to be queued.
	wp.mu.Lock()
	if wp.closed {
		wp.mu.Unlock()
		return false
	}
	wp.inflight++
	wp.mu.Unlock()

	select {
	case wp.tasks <- task:
		return true
	case <-ctx.Done():
		wp.finish()
		return false
	}
}

// Drain blocks until every task accepted so far has finished. It may be
// called repeatedly, and tasks can be submitted again afterwards.
func (wp *WorkPool) Drain() {
	wp.mu.Lock()
	for wp.inflight > 0 {
		wp.cond.Wait()
	}
	wp.mu.Unlock()
}

// Stop stops accepting new tasks, waits for every accepted task to
// finish (cancel the Start context first to abandon queued work), shuts
// the workers down and returns the errors collected during the run,
// including recovered panics. It is safe to call more than once; later
// calls return the same errors. If WithErrorHandler is set the returned
// slice is empty.
func (wp *WorkPool) Stop() []error {
	wp.stopOne.Do(func() {
		wp.mu.Lock()
		wp.closed = true
		for wp.inflight > 0 {
			wp.cond.Wait()
		}
		limiter := wp.limiter
		wp.mu.Unlock()

		close(wp.stopCh)
		wp.wg.Wait()
		if limiter != nil {
			limiter.Stop()
		}
	})

	wp.mu.Lock()
	defer wp.mu.Unlock()
	return append([]error(nil), wp.errs...)
}

func (wp *WorkPool) worker() {
	defer wp.wg.Done()
	for {
		select {
		case task := <-wp.tasks:
			wp.run(task)
		case <-wp.stopCh:
			return
		}
	}
}

// run executes one accepted task and always marks it finished, after
// its error (if any) has been recorded.
func (wp *WorkPool) run(task Task) {
	defer wp.finish()

	if err := wp.ctx.Err(); err != nil {
		// accepted but cancelled before it started: report, never drop
		wp.recordFailure(&result.PoolError{TaskType: typeName(task), Attempts: 0, Err: err})
		return
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
		wp.record(err)
	}
}

func (wp *WorkPool) recordFailure(err error) {
	if wp.metrics != nil {
		wp.metrics.processed.Add(1)
		wp.metrics.failed.Add(1)
	}
	wp.record(err)
}

func (wp *WorkPool) record(err error) {
	if wp.onError != nil {
		_ = result.CatchErr(func() error { wp.onError(err); return nil })
		return
	}
	wp.mu.Lock()
	wp.errs = append(wp.errs, err)
	wp.mu.Unlock()
}

func (wp *WorkPool) finish() {
	wp.mu.Lock()
	wp.inflight--
	if wp.inflight == 0 {
		wp.cond.Broadcast()
	}
	wp.mu.Unlock()
}

// runWithRetry runs task.Process up to 1+maxRetries times, stopping
// early on a panic or when the context is cancelled. It returns a
// PoolError wrapping the last error if every attempt failed.
func (wp *WorkPool) runWithRetry(task Task) error {
	var err error
	attempts := 0
	for attempt := 0; attempt <= wp.maxRetries; attempt++ {
		attempts++
		err = result.CatchErr(task.Process)
		if err == nil {
			return nil
		}

		var pe *result.PanicError
		if errors.As(err, &pe) || attempt == wp.maxRetries || wp.ctx.Err() != nil {
			break
		}
		if wp.backoff != nil {
			timer := time.NewTimer(wp.backoff(attempt))
			select {
			case <-timer.C:
			case <-wp.ctx.Done():
				timer.Stop()
				return &result.PoolError{TaskType: typeName(task), Attempts: attempts, Err: err}
			}
		}
	}
	return &result.PoolError{TaskType: typeName(task), Attempts: attempts, Err: err}
}

// typeName returns the task's type name without panicking for value
// types, pointers, func types or anything else.
func typeName(task Task) string {
	t := reflect.TypeOf(task)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if name := t.Name(); name != "" {
		return name
	}
	return t.String()
}
