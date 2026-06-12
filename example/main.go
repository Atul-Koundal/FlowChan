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
	"FlowChan/termination"
	"FlowChan/retry"
)

// --- pool tasks ---

type EmailTask struct {
	To      string
	Subject string
}

func (t *EmailTask) Process() error {
	fmt.Printf("  [email] sending to %s\n", t.To)
	time.Sleep(100 * time.Millisecond)
	return nil
}

type ResizeTask struct {
	Filename string
	Width    int
	Height   int
}

func (t *ResizeTask) Process() error {
	fmt.Printf("  [resize] %s to %dx%d\n", t.Filename, t.Width, t.Height)
	return nil
}

// flakyTask fails the first two attempts then succeeds
type flakyTask struct {
	name     string
	attempts int
}

func (t *flakyTask) Process() error {
	t.attempts++
	if t.attempts < 3 {
		fmt.Printf("  [flaky] %s attempt %d failed\n", t.name, t.attempts)
		return fmt.Errorf("temporary failure")
	}
	fmt.Printf("  [flaky] %s succeeded on attempt %d\n", t.name, t.attempts)
	return nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Println("^^^^^ 1. Worker Pool with Retries ^^^^^")
	runPool(ctx)

	fmt.Println("\n^^^^^ 2. Pipeline - Ordered Stage ^^^^^")
	runOrderedPipeline(ctx)

	fmt.Println("\n^^^^^ 3. Batch - Fixed Size + Timeout ^^^^^")
	runBatch(ctx)

	fmt.Println("\n^^^^^ 4. Batch - Realtime Sliding Window ^^^^^")
	runRealtimeBatch(ctx)

	fmt.Println("\n^^^^^ 5. Stream - FlatMap ^^^^^")
	runFlatMap(ctx)

	fmt.Println("\n^^^^^ 6. Stream - Backpressure ^^^^^")
	runBackpressure(ctx)

	fmt.Println("\n^^^^^ 7. Iterators ^^^^^")
	runIter(ctx)

	fmt.Println("\n^^^^^ 8. Graceful Termination ^^^^^")
	runTermination()

	fmt.Println("\n^^^^^ 9. Retry with Backoff ^^^^^")
	runRetry(ctx)
}

func runPool(ctx context.Context) {
	tasks := []pool.Task{
		&EmailTask{To: "alice@example.com", Subject: "Welcome"},
		&ResizeTask{Filename: "photo.jpg", Width: 800, Height: 600},
		&flakyTask{name: "report-job"},
	}

	wp := pool.NewWorkPool(tasks, 3, pool.WithRetries(3))
	errs := wp.Run(ctx)
	if len(errs) > 0 {
		fmt.Println("  errors:", errs)
	} else {
		fmt.Println("  all tasks completed successfully")
	}
}

func runOrderedPipeline(ctx context.Context) {
	// items processed concurrently but output order is guaranteed
	stage := pipeline.NewOrderedStage(5, func(ctx context.Context, n int) (string, error) {
		// simulate variable processing time
		time.Sleep(time.Duration(10-n) * time.Millisecond)
		return fmt.Sprintf("item-%d", n*10), nil
	})

	in := make(chan int, 5)
	go func() {
		defer close(in)
		for i := 1; i <= 5; i++ {
			in <- i
		}
	}()

	fmt.Println("  output (should be item-10 through item-50 in order):")
	for r := range stage.Run(ctx, in) {
		if r.IsErr() {
			fmt.Println("  error:", r.Err)
			continue
		}
		fmt.Println(" ", r.Value)
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

func runRealtimeBatch(ctx context.Context) {
	in := make(chan int)
	go func() {
		defer close(in)
		// burst of 3 items
		for i := 1; i <= 3; i++ {
			in <- i
		}
		// silence - sliding window fires
		time.Sleep(200 * time.Millisecond)
		// another burst
		for i := 4; i <= 6; i++ {
			in <- i
		}
		// silence - sliding window fires again
		time.Sleep(200 * time.Millisecond)
	}()

	b := batch.NewRealtime[int](100, 100*time.Millisecond)
	for r := range b.Run(ctx, in) {
		if r.IsErr() {
			fmt.Println("  error:", r.Err)
			continue
		}
		fmt.Println("  realtime batch:", r.Value)
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

func runBackpressure(ctx context.Context) {
	in := make(chan int, 20)
	go func() {
		defer close(in)
		for i := 1; i <= 10; i++ {
			in <- i
		}
	}()

	// only 3 items in-flight at once regardless of input size
	out := stream.BackpressureMap(ctx, in, 3,
		func(ctx context.Context, n int) (int, error) {
			time.Sleep(20 * time.Millisecond)
			return n * 2, nil
		})

	fmt.Println("  results (max 3 concurrent):")
	for r := range out {
		if r.IsErr() {
			fmt.Println("  error:", r.Err)
			continue
		}
		fmt.Println(" ", r.Value)
	}
}

func runIter(ctx context.Context) {
	ch := make(chan int, 10)
	go func() {
		defer close(ch)
		for i := 1; i <= 10; i++ {
			ch <- i
		}
	}()

	seq := iter.FromChan(ctx, ch)
	evens := iter.Filter(seq, func(n int) bool { return n%2 == 0 })
	doubled := iter.Map(evens, func(n int) int { return n * 2 })
	results := iter.Collect(doubled)

	fmt.Println("  evens doubled:", results)
}

func runTermination() {
	term := termination.New()

	// simulate 5 in-flight workers
	for i := 1; i <= 5; i++ {
		if !term.Track() {
			fmt.Println("  rejected - already stopped")
			continue
		}
		workerID := i
		go func() {
			defer term.Done()
			time.Sleep(100 * time.Millisecond)
			fmt.Printf("  worker %d finished\n", workerID)
		}()
	}

	

	// signal stop after 50ms - workers are still running
	go func() {
		time.Sleep(50 * time.Millisecond)
		fmt.Println("  stop signal sent")
		term.Stop()
	}()

	// Wait blocks until stop is called AND all workers drain
	term.Wait()
	fmt.Println("  all in-flight work drained, shutdown complete")
}

func runRetry(ctx context.Context) {
	in := make(chan int, 5)
	go func() {
		defer close(in)
		for i := 1; i <= 5; i++ {
			in <- i
		}
	}()

	attempts := make(map[int]int)

	out := retry.Stream(ctx, in, 4,
		retry.ExponentialJitter(10*time.Millisecond, 100*time.Millisecond),
		func(ctx context.Context, n int) (int, error) {
			attempts[n]++
			if attempts[n] < 3 {
				fmt.Printf("  item %d attempt %d failed\n", n, attempts[n])
				return 0, fmt.Errorf("temporary failure")
			}
			fmt.Printf("  item %d succeeded on attempt %d\n", n, attempts[n])
			return n * 2, nil
		})

	for r := range out {
		if r.IsErr() {
			fmt.Println("  final error:", r.Err)
			continue
		}
		fmt.Println("  result:", r.Value)
	}
}