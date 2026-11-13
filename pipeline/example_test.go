package pipeline_test

import (
	"context"
	"fmt"
	"strconv"

	"github.com/Atul-Koundal/FlowChan/pipeline"
)

func ExampleChain() {
	in := make(chan string, 3)
	for _, s := range []string{"1", "2", "x"} {
		in <- s
	}
	close(in)

	parse := pipeline.NewStage(1, func(ctx context.Context, s string) (int, error) {
		return strconv.Atoi(s)
	}, pipeline.WithName[string, int]("parse"))
	double := pipeline.NewStage(1, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})

	sum, failed := 0, 0
	for r := range pipeline.Chain(parse, double).Run(context.Background(), in) {
		if r.IsErr() {
			failed++
			continue
		}
		sum += r.Value
	}
	fmt.Println("sum:", sum, "failed:", failed)
	// Output: sum: 6 failed: 1
}
