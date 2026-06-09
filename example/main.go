package main

import (
	"context"
	"fmt"
	"time"

	"FlowChan/batch"
	"FlowChan/iter"
	"FlowChan/pipeline"
	"FlowChan/pool"
	"FlowChan/stream"
)

// --- pool tasks ---

type EmailTask struct {
	To      string
	Subject string
}

func (t *EmailTask) Process() error {
	fmt.Printf("  [email] sending to %s: %s\n", t.To, t.Subject)
	time.Sleep(100 * time.Millisecond)
	return nil
}

type ResizeTask struct {
	Filename string
	Width    int
	Height   int
}

func (t *ResizeTask) Process() error {
	fmt.Printf("  [resize] %s → %dx%d\n", t.Filename, t.Width, t.Height)
	time.Sleep(50 * time.Millisecond)
	return nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Println("━━━ 1. Worker Pool ━━━")
	runPool(ctx)

	fmt.Println("\n━━━ 2. Pipeline ━━━")
	runPipeline(ctx)

	fmt.Println("\n━━━ 3. Batch ━━━")
	runBatch(ctx)

	fmt.Println("\n━━━ 4. Stream FlatMap ━━━")
	runFlatMap(ctx)

	fmt.Println("\n━━━ 5. Iterators ━━━")
	runIter(ctx)
}

func runPool(ctx context.Context) {
	tasks := []pool.Task{
		&EmailTask{To: "alice@example.com", Subject: "Welcome"},
		&EmailTask{To: "bob@example.com", Subject: "Update"},
		&ResizeTask{Filename: "photo.jpg", Width: 800, Height: 600},
		&ResizeTask{Filename: "banner.png", Width: 1200, Height: 400},
	}

	wp := pool.NewWorkPool(tasks, 3)
	errs := wp.Run(ctx)
	if len(errs) > 0 {
		fmt.Println("errors:", errs)
	} else {
		fmt.Println("  all tasks completed")
	}
}

func runPipeline(ctx context.Context) {
	// stage 1: double the number
	stage1 := pipeline.NewStage(3, func(ctx context.Context, n int) (int, error) {
		fmt.Printf("  [stage1] %d → %d\n", n, n*2)
		return n * 2, nil
	})

	// stage 2: format as string
	stage2 := pipeline.NewStage(3, func(ctx context.Context, n int) (string, error) {
		result := fmt.Sprintf("item-%d", n)
		fmt.Printf("  [stage2] %d → %s\n", n, result)
		return result, nil
	})

	in := make(chan int, 5)
	go func() {
		defer close(in)
		for i := 1; i <= 5; i++ {
			in <- i
		}
	}()

	p := pipeline.Chain(stage1, stage2)
	for r := range p.Run(ctx, in) {
		if r.IsErr() {
			fmt.Println("  error:", r.Err)
			continue
		}
		fmt.Println("  final:", r.Value)
	}
}

func runBatch(ctx context.Context) {
	in := make(chan int, 10)
	go func() {
		defer close(in)
		for i := 1; i <= 10; i++ {
			in <- i
		}
	}()

	b := batch.New[int](3, 500*time.Millisecond)
	for r := range b.Run(ctx, in) {
		if r.IsErr() {
			fmt.Println("  error:", r.Err)
			continue
		}
		fmt.Println("  batch:", r.Value)
	}
}

func runFlatMap(ctx context.Context) {
	in := make(chan int, 3)
	go func() {
		defer close(in)
		in <- 1
		in <- 2
		in <- 3
	}()

	// each number expands into [n, n*10, n*100]
	out := stream.FlatMap(ctx, in, 2,
		func(ctx context.Context, n int) ([]int, error) {
			return []int{n, n * 10, n * 100}, nil
		})

	for r := range out {
		if r.IsErr() {
			fmt.Println("  error:", r.Err)
			continue
		}
		fmt.Println("  value:", r.Value)
	}
}

func runIter(ctx context.Context) {
	ch := make(chan int, 5)
	go func() {
		defer close(ch)
		for i := 1; i <= 5; i++ {
			ch <- i
		}
	}()

	// chain: fromChan → filter evens → double → collect
	seq := iter.FromChan(ctx, ch)
	evens := iter.Filter(seq, func(n int) bool { return n%2 == 0 })
	doubled := iter.Map(evens, func(n int) int { return n * 2 })
	results := iter.Collect(doubled)

	fmt.Println("  evens doubled:", results)
}