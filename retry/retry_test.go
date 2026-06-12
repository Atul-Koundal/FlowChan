package retry

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestFixed_AlwaysSameWait(t *testing.T) {
	strategy := Fixed(100 * time.Millisecond)
	for i := 0; i < 5; i++ {
		if strategy(i) != 100*time.Millisecond {
			t.Errorf("attempt %d: expected 100ms, got %v", i, strategy(i))
		}
	}
}

func TestExponential_Doubles(t *testing.T) {
	strategy := Exponential(100*time.Millisecond, 10*time.Second)
	expected := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
	}
	for i, exp := range expected {
		got := strategy(i)
		if got != exp {
			t.Errorf("attempt %d: expected %v, got %v", i, exp, got)
		}
	}
}

func TestExponential_CapsAtMax(t *testing.T) {
	strategy := Exponential(1*time.Second, 3*time.Second)
	// after several doublings it should never exceed max
	for i := 0; i < 10; i++ {
		got := strategy(i)
		if got > 3*time.Second {
			t.Errorf("attempt %d: %v exceeded max of 3s", i, got)
		}
	}
}

func TestExponentialJitter_WithinBounds(t *testing.T) {
	strategy := ExponentialJitter(100*time.Millisecond, 5*time.Second)
	for i := 0; i < 10; i++ {
		got := strategy(i)
		if got <= 0 {
			t.Errorf("attempt %d: wait must be positive, got %v", i, got)
		}
		if got > 5*time.Second {
			t.Errorf("attempt %d: %v exceeded max of 5s", i, got)
		}
	}
}

func TestDo_SuccessOnFirstAttempt(t *testing.T) {
	calls := 0
	err := Do(context.Background(), 3, Fixed(time.Millisecond), func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

func TestDo_RetriesUntilSuccess(t *testing.T) {
	calls := 0
	err := Do(context.Background(), 5, Fixed(time.Millisecond), func() error {
		calls++
		if calls < 3 {
			return fmt.Errorf("not yet")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

func TestDo_ExhaustsAttempts(t *testing.T) {
	calls := 0
	err := Do(context.Background(), 3, Fixed(time.Millisecond), func() error {
		calls++
		return fmt.Errorf("always fails")
	})
	if err == nil {
		t.Fatal("expected error after exhausting attempts")
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}

	var retryErr *RetryError
	if !errors.As(err, &retryErr) {
		t.Errorf("expected RetryError, got %T", err)
	}
	if retryErr.Attempts != 3 {
		t.Errorf("expected 3 attempts in error, got %d", retryErr.Attempts)
	}
}

func TestDo_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	calls := 0
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := Do(ctx, 10, Fixed(100*time.Millisecond), func() error {
		calls++
		return fmt.Errorf("always fails")
	})

	if err == nil {
		t.Fatal("expected error from context cancellation")
	}
	if calls > 2 {
		t.Errorf("context cancelled but still made %d calls", calls)
	}
}

func TestDoWithResult_ReturnsValue(t *testing.T) {
	val, err := DoWithResult(context.Background(), 3, Fixed(time.Millisecond),
		func() (string, error) {
			return "hello", nil
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "hello" {
		t.Errorf("expected 'hello', got %q", val)
	}
}

func TestDoWithResult_RetriesAndReturns(t *testing.T) {
	attempts := 0
	val, err := DoWithResult(context.Background(), 5, Fixed(time.Millisecond),
		func() (int, error) {
			attempts++
			if attempts < 3 {
				return 0, fmt.Errorf("not ready")
			}
			return 42, nil
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 42 {
		t.Errorf("expected 42, got %d", val)
	}
}

func TestStream_RetriesAndEmitsSuccess(t *testing.T) {
	in := make(chan int, 3)
	in <- 1
	in <- 2
	in <- 3
	close(in)

	attempts := make(map[int]int)

	out := Stream(context.Background(), in, 3, Fixed(time.Millisecond),
		func(ctx context.Context, n int) (int, error) {
			attempts[n]++
			if attempts[n] < 2 {
				return 0, fmt.Errorf("not ready")
			}
			return n * 2, nil
		})

	var results []int
	var errs []error
	for r := range out {
		if r.IsErr() {
			errs = append(errs, r.Err)
		} else {
			results = append(results, r.Value)
		}
	}

	if len(errs) != 0 {
		t.Errorf("expected no errors, got %d", len(errs))
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
}

func TestStream_EmitsErrorAfterExhaustion(t *testing.T) {
	in := make(chan int, 2)
	in <- 1
	in <- 2
	close(in)

	out := Stream(context.Background(), in, 2, Fixed(time.Millisecond),
		func(ctx context.Context, n int) (int, error) {
			return 0, fmt.Errorf("always fails")
		})

	var errCount int
	for r := range out {
		if r.IsErr() {
			errCount++
		}
	}

	if errCount != 2 {
		t.Errorf("expected 2 errors, got %d", errCount)
	}
}