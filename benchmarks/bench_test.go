package benchmarks

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Atul-Koundal/FlowChan/batch"
	"github.com/Atul-Koundal/FlowChan/pipeline"
	"github.com/Atul-Koundal/FlowChan/pool"
	"github.com/Atul-Koundal/FlowChan/retry"
	"github.com/Atul-Koundal/FlowChan/stream"
)

// helpers

func makeIntChan(n int) <-chan int {
	ch := make(chan int, n)
	for i := 0; i < n; i++ {
		ch <- i
	}
	close(ch)
	return ch
}

type noopTask struct{}

func (t *noopTask) Process() error { return nil }

func makeTasks(n int) []pool.Task {
	tasks := make([]pool.Task, n)
	for i := range tasks {
		tasks[i] = &noopTask{}
	}
	return tasks
}

// pool benchmarks

func BenchmarkPool_10Tasks_3Workers(b *testing.B) {
	for i := 0; i < b.N; i++ {
		wp := pool.NewWorkPool(makeTasks(10), 3)
		wp.Run(context.Background())
	}
}

func BenchmarkPool_100Tasks_10Workers(b *testing.B) {
	for i := 0; i < b.N; i++ {
		wp := pool.NewWorkPool(makeTasks(100), 10)
		wp.Run(context.Background())
	}
}

func BenchmarkPool_1000Tasks_50Workers(b *testing.B) {
	for i := 0; i < b.N; i++ {
		wp := pool.NewWorkPool(makeTasks(1000), 50)
		wp.Run(context.Background())
	}
}

// pipeline benchmarks

func BenchmarkStage_100Items_5Workers(b *testing.B) {
	fn := func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	}

	for i := 0; i < b.N; i++ {
		stage := pipeline.NewStage(5, fn)
		out := stage.Run(context.Background(), makeIntChan(100))
		for range out {
		}
	}
}

func BenchmarkChain_100Items(b *testing.B) {
	stage1 := pipeline.NewStage(5, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})
	stage2 := pipeline.NewStage(5, func(ctx context.Context, n int) (string, error) {
		return fmt.Sprintf("v-%d", n), nil
	})
	p := pipeline.Chain(stage1, stage2)

	for i := 0; i < b.N; i++ {
		out := p.Run(context.Background(), makeIntChan(100))
		for range out {
		}
	}
}

func BenchmarkOrderedStage_100Items_5Workers(b *testing.B) {
	stage := pipeline.NewOrderedStage(5, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})

	for i := 0; i < b.N; i++ {
		out := stage.Run(context.Background(), makeIntChan(100))
		for range out {
		}
	}
}

// batch benchmarks

func BenchmarkBatch_1000Items_Size10(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bt := batch.New[int](10, 1*time.Second)
		out := bt.Run(context.Background(), makeIntChan(1000))
		for range out {
		}
	}
}

func BenchmarkRealtimeBatch_1000Items(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bt := batch.NewRealtime[int](10, 50*time.Millisecond)
		out := bt.Run(context.Background(), makeIntChan(1000))
		for range out {
		}
	}
}

// stream benchmarks

func BenchmarkMap_100Items_5Workers(b *testing.B) {
	fn := func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	}

	for i := 0; i < b.N; i++ {
		out := stream.Map(context.Background(), makeIntChan(100), 5, fn)
		for range out {
		}
	}
}

func BenchmarkOrderedMap_100Items_5Workers(b *testing.B) {
	fn := func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	}

	for i := 0; i < b.N; i++ {
		out := stream.OrderedMap(context.Background(), makeIntChan(100), 5, fn)
		for range out {
		}
	}
}

func BenchmarkFlatMap_100Items_3Workers(b *testing.B) {
	fn := func(ctx context.Context, n int) ([]int, error) {
		return []int{n, n * 2, n * 3}, nil
	}

	for i := 0; i < b.N; i++ {
		out := stream.FlatMap(context.Background(), makeIntChan(100), 3, fn)
		for range out {
		}
	}
}

func BenchmarkBackpressureMap_100Items_5Workers(b *testing.B) {
	fn := func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	}

	for i := 0; i < b.N; i++ {
		out := stream.BackpressureMap(context.Background(), makeIntChan(100), 5, fn)
		for range out {
		}
	}
}

// retry benchmarks

func BenchmarkDo_NoRetry(b *testing.B) {
	for i := 0; i < b.N; i++ {
		retry.Do(context.Background(), 3, retry.Fixed(time.Millisecond), func() error {
			return nil
		})
	}
}

func BenchmarkRetryStream_100Items(b *testing.B) {
	fn := func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	}

	for i := 0; i < b.N; i++ {
		out := retry.Stream(context.Background(), makeIntChan(100), 3,
			retry.Fixed(time.Millisecond), fn)
		for range out {
		}
	}
}