package pool_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Atul-Koundal/FlowChan/pool"
)

type greet struct {
	name  string
	count *atomic.Int32
}

func (g *greet) Process() error {
	g.count.Add(1)
	return nil
}

func ExampleWorkPool() {
	var done atomic.Int32
	wp := pool.NewWorkPool(4,
		pool.WithRetries(2),
		pool.WithRetryBackoff(func(int) time.Duration { return 10 * time.Millisecond }),
	)
	wp.Start(context.Background())

	for _, n := range []string{"ada", "grace", "linus"} {
		wp.Submit(&greet{name: n, count: &done})
	}

	wp.Drain()        // wait for everything submitted so far
	errs := wp.Stop() // safe to call more than once
	fmt.Println("done:", done.Load(), "errors:", len(errs))
	// Output: done: 3 errors: 0
}
