package pool

import (
	"context"
	"sync"

	"go.uber.org/goleak"

)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type Task interface {
	Process() error
}


type WorkPool struct {
	tasks       []Task
	concurrency int
	maxRetries  int  //added a max retries option
	tasksChan   chan Task
	errors      chan error
}

// Pool is a pool of goroutines used to execute tasks concurrently.
//
// Tasks are submitted with Go(),which makes it a goroutine
//  Once all your tasks have been submitted, you
// must call Wait() to clean up any spawned goroutines and propagate any
// panics.
//
// Goroutines are started lazily, so creating a new pool is cheap. There will
// never be more goroutines spawned than there are tasks submitted.
//
// The configuration methods (With*) will panic if they are used after calling
// Go() for the first time.
//


type Option func(*WorkPool)

func WithRetries(n int) Option {
	return func(wp *WorkPool) {
		wp.maxRetries = n
	}
}

//A new pool is created via this function
func NewWorkPool(tasks []Task, concurrency int, opts ...Option) *WorkPool {
	wp := &WorkPool{
		tasks:       tasks,
		concurrency: concurrency,
		maxRetries:  0, // no retries by default
		errors:      make(chan error, len(tasks)),
	}
	for _, o := range opts {
		o(wp)
	}
	return wp
}

func (wp *WorkPool) runWithRetry(task Task) error {
	var err error
	// attempt = original run + retries
	for attempt := 0; attempt <= wp.maxRetries; attempt++ {
		err = task.Process()
		if err == nil {
			return nil // success
		}
	}
	return err // return last error after all attempts exhausted
}



func (wp *WorkPool) worker(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done() //called when worker exits for any reason
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

func (wp *WorkPool) Run(ctx context.Context) []error {
	wp.tasksChan = make(chan Task, len(wp.tasks))
	//Waitgroup tracks workers not tasks

	var wg sync.WaitGroup
	for i := 0; i < wp.concurrency; i++ {
		wg.Add(1)
		go wp.worker(ctx, &wg)
	}

	// send jobs
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

	// wait for all workers to finish then close errors it 
	//  cleans up spawned goroutines, propagating any panics that were
	// raised by any task.
	wg.Wait()
	close(wp.errors)

	var errs []error
	for err := range wp.errors {
		errs = append(errs, err)
	}
	return errs
}

// Pool is efficient, but not zero cost. It should not be used for very short
// tasks. Startup and teardown come with an overhead of around 1µs, and each
// task has an overhead of around 300ns.