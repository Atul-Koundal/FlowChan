package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	fresult "github.com/Atul-Koundal/FlowChan/result"
)

func TestExponential_NoOverflowAtHighAttempts(t *testing.T) {
	s := Exponential(time.Second, 30*time.Second)
	for _, attempt := range []int{33, 34, 62, 63, 64, 70, 1000, 1 << 30} {
		if got := s(attempt); got != 30*time.Second {
			t.Fatalf("attempt %d: want 30s cap, got %v", attempt, got)
		}
	}
}

func TestExponentialJitter_NeverPanicsAndStaysInRange(t *testing.T) {
	cases := []struct{ base, max time.Duration }{
		{time.Second, 30 * time.Second},
		{time.Nanosecond, time.Second},
		{0, time.Second},
		{time.Millisecond, 0},
	}
	for _, c := range cases {
		s := ExponentialJitter(c.base, c.max)
		for attempt := 0; attempt < 200; attempt++ {
			got := s(attempt)
			if got < 0 {
				t.Fatalf("%+v attempt %d: negative wait %v", c, attempt, got)
			}
			if c.max > 0 && got > c.max {
				t.Fatalf("%+v attempt %d: %v exceeds max", c, attempt, got)
			}
		}
	}
}

func TestExponentialJitter_StillVariesAtCap(t *testing.T) {
	s := ExponentialJitter(time.Second, 30*time.Second)
	seen := map[time.Duration]bool{}
	for i := 0; i < 200; i++ {
		seen[s(20)] = true
	}
	if len(seen) < 10 {
		t.Fatalf("jitter vanished at the cap: only %d distinct waits", len(seen))
	}
}

func TestDo_NonPositiveAttemptsRunsOnce(t *testing.T) {
	calls := 0
	err := Do(context.Background(), 0, Fixed(0), func() error { calls++; return errors.New("x") })
	if calls != 1 || err == nil {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestDo_PanicIsNotRetriedAndNotFatal(t *testing.T) {
	calls := 0
	err := Do(context.Background(), 5, Fixed(0), func() error { calls++; panic("bug") })
	var pe *fresult.PanicError
	if !errors.As(err, &pe) || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestDo_CancelKeepsLastError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sentinel := errors.New("upstream down")
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	err := Do(ctx, 10, Fixed(time.Hour), func() error { return sentinel })
	if !errors.Is(err, context.Canceled) || !errors.Is(err, sentinel) {
		t.Fatalf("want both ctx error and last error, got %v", err)
	}
}

func TestDo_AlreadyCancelledDoesNotCallFn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	err := Do(ctx, 3, Fixed(0), func() error { called = true; return nil })
	if called || !errors.Is(err, context.Canceled) {
		t.Fatalf("called=%v err=%v", called, err)
	}
}

func TestWrap_RetriesInsideCallerWorkers(t *testing.T) {
	attempts := 0
	fn := Wrap(3, Fixed(0), func(ctx context.Context, n int) (int, error) {
		attempts++
		if attempts < 3 {
			return 0, errors.New("transient")
		}
		return n * 2, nil
	})
	got, err := fn(context.Background(), 21)
	if err != nil || got != 42 || attempts != 3 {
		t.Fatalf("got=%d err=%v attempts=%d", got, err, attempts)
	}
}
