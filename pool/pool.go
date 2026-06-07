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
    wg          sync.WaitGroup
}

func NewWorkPool(tasks []Task, concurrency int) *WorkPool {
    return &WorkPool{
        tasks:       tasks,
        concurrency: concurrency,
        errors:      make(chan error, len(tasks)),
    }
}

func (wp *WorkPool) worker(ctx context.Context) {
    for {
        select {
        case task, ok := <-wp.tasksChan:
            if !ok {
                return
            }
            if err := task.Process(); err != nil {
                wp.errors <- err
            }
            wp.wg.Done()
        case <-ctx.Done():
            return
        }
    }
}

func (wp *WorkPool) Run(ctx context.Context) []error {
    wp.tasksChan = make(chan Task, len(wp.tasks))

    for i := 0; i < wp.concurrency; i++ {
        go wp.worker(ctx)
    }

    wp.wg.Add(len(wp.tasks))
    for _, task := range wp.tasks {
        wp.tasksChan <- task
    }
    close(wp.tasksChan)

    wp.wg.Wait()
    close(wp.errors)

    var errs []error
    for err := range wp.errors {
        errs = append(errs, err)
    }
    return errs
}