package pool

import (
	"context"
	"sync"
)

type Task interface {
	Process() error
}

type WorkPool struct {
	tasks       []Task
	concurrency int
	tasksChan   chan Task
	errors      chan error
}

func NewWorkPool(tasks []Task, concurrency int) *WorkPool {
	return &WorkPool{
		tasks:       tasks,
		concurrency: concurrency,
		errors:      make(chan error, len(tasks)),
	}
}

func (wp *WorkPool) worker(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done() // called when worker exits for ANY reason
	for {
		select {
		case task, ok := <-wp.tasksChan:
			if !ok {
				return
			}
			if err := task.Process(); err != nil {
				wp.errors <- err
			}
		case <-ctx.Done():
			return
		}
	}
}

func (wp *WorkPool) Run(ctx context.Context) []error {
	wp.tasksChan = make(chan Task, len(wp.tasks))

	// WaitGroup tracks WORKERS not tasks
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

	// wait for all workers to finish then close errors
	wg.Wait()
	close(wp.errors)

	var errs []error
	for err := range wp.errors {
		errs = append(errs, err)
	}
	return errs
}