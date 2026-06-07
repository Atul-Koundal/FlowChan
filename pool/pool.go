package pool

import (
    "context"
    "fmt"
    "sync"
    "time"
)

type Task struct {
    ID int
}

func (t *Task) Process() {
    fmt.Printf("processing task %d\n", t.ID)
    time.Sleep(5 * time.Second)
}

type WorkPool struct {
    tasks       []Task
    concurrency int
    tasksChan   chan Task
    wg          sync.WaitGroup
}

func NewWorkPool(tasks []Task, concurrency int) *WorkPool {
    return &WorkPool{
        tasks:       tasks,
        concurrency: concurrency,
    }
}

func (wp *WorkPool) worker(ctx context.Context) {
    for {
        select {
        case task, ok := <-wp.tasksChan:
            if !ok {
                return
            }
            task.Process()
            wp.wg.Done()
        case <-ctx.Done():
            return
        }
    }
}

func (wp *WorkPool) Run(ctx context.Context) {
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
}