package retry

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

var ErrPermanent = errors.New("permanent error")

func TestDo_StopsOnNonRetryableError(t *testing.T) {
	calls := 0
	err := Do(context.Background(), 5, Fixed(time.Millisecond),
		func() error {
			calls++
			return ErrPermanent
		},
		func(err error) bool {
			return !errors.Is(err, ErrPermanent)
		},
	)

	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 1 {
		t.Errorf("expected 1 call for non-retryable error, got %d", calls)
	}
}

func TestDo_RetriesOnRetryableError(t *testing.T) {
	calls := 0
	err := Do(context.Background(), 5, Fixed(time.Millisecond),
		func() error {
			calls++
			if calls < 3 {
				return fmt.Errorf("temporary")
			}
			return nil
		},
		AlwaysRetry,
	)

	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

func TestStream_ShouldRetryPredicate(t *testing.T) {
	in := make(chan int, 3)
	in <- 1
	in <- 2
	in <- 3
	close(in)

	calls := make(map[int]int)

	out := Stream(context.Background(), in, 5,
		Fixed(time.Millisecond),
		func(ctx context.Context, n int) (int, error) {
			calls[n]++
			if n == 2 {
				return 0, ErrPermanent // non-retryable
			}
			return n * 2, nil
		},
		func(err error) bool {
			return !errors.Is(err, ErrPermanent)
		},
	)

	var errCount, valCount int
	for r := range out {
		if r.IsErr() {
			errCount++
		} else {
			valCount++
		}
	}

	if errCount != 1 {
		t.Errorf("expected 1 error, got %d", errCount)
	}
	if valCount != 2 {
		t.Errorf("expected 2 values, got %d", valCount)
	}
	// item 2 should only have been called once since it's non-retryable
	if calls[2] != 1 {
		t.Errorf("expected 1 call for non-retryable item, got %d", calls[2])
	}
}