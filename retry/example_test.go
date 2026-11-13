package retry_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Atul-Koundal/FlowChan/retry"
)

func ExampleDo() {
	attempts := 0
	err := retry.Do(context.Background(), 5,
		retry.ExponentialJitter(time.Millisecond, 10*time.Millisecond),
		func() error {
			attempts++
			if attempts < 3 {
				return errors.New("temporary")
			}
			return nil
		})
	fmt.Println("attempts:", attempts, "err:", err)
	// Output: attempts: 3 err: <nil>
}

func ExampleWrap() {
	calls := 0
	fn := retry.Wrap(3, retry.Fixed(0), func(ctx context.Context, n int) (int, error) {
		calls++
		if calls == 1 {
			return 0, errors.New("flaky")
		}
		return n * 2, nil
	})
	v, err := fn(context.Background(), 21)
	fmt.Println(v, err)
	// Output: 42 <nil>
}
