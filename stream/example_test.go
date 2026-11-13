package stream_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/Atul-Koundal/FlowChan/result"
	"github.com/Atul-Koundal/FlowChan/stream"
)

func ExampleOrderedMap() {
	in := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		in <- i
	}
	close(in)

	out := stream.OrderedMap(context.Background(), in, 3, func(ctx context.Context, n int) (int, error) {
		if n == 4 {
			panic("bad input") // recovered: becomes an error, never crashes the process
		}
		return n * n, nil
	})
	for r := range out {
		var pe *result.PanicError
		if errors.As(r.Err, &pe) {
			fmt.Println("recovered panic:", pe.Value)
			continue
		}
		fmt.Println(r.Value)
	}
	// Output:
	// 1
	// 4
	// 9
	// recovered panic: bad input
	// 25
}
