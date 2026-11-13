package termination_test

import (
	"context"
	"fmt"
	"time"

	"github.com/Atul-Koundal/FlowChan/termination"
)

func ExampleTerminator_WaitContext() {
	term := termination.New()

	if term.Track() {
		go func() {
			defer term.Done()
			time.Sleep(10 * time.Millisecond) // in-flight work
		}()
	}

	term.Stop() // no new work accepted from here on

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fmt.Println("drained cleanly:", term.WaitContext(ctx) == nil)
	// Output: drained cleanly: true
}
