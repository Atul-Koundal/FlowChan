package termination

import (
	"context"
	"errors"
	"testing"
	"time"
)

// goleak (TestMain) fails the package if the goroutine leaks.
func TestWithTerminator_CancelReleasesGoroutine(t *testing.T) {
	for i := 0; i < 50; i++ {
		_, cancel := WithTerminator(context.Background(), New())
		cancel()
	}
}

func TestWithTerminator_CancelsAfterDrain(t *testing.T) {
	term := New()
	ctx, cancel := WithTerminator(context.Background(), term)
	defer cancel()
	term.Track()
	term.Stop()
	select {
	case <-ctx.Done():
		t.Fatal("context cancelled before in-flight work finished")
	case <-time.After(30 * time.Millisecond):
	}
	term.Done()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("context not cancelled after drain")
	}
}

func TestWaitContext_TimesOutOnStuckWork(t *testing.T) {
	term := New()
	term.Track() // never Done
	term.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := term.WaitContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	term.Done() // let the test clean up
}

func TestWaitContext_ReturnsNilWhenDrained(t *testing.T) {
	term := New()
	term.Track()
	go func() { time.Sleep(20 * time.Millisecond); term.Done() }()
	term.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := term.WaitContext(ctx); err != nil {
		t.Fatalf("got %v", err)
	}
}

func TestWaitContext_NotStoppedYetBlocksUntilContext(t *testing.T) {
	term := New()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := term.WaitContext(ctx); err == nil {
		t.Fatal("must not report drained before Stop")
	}
}

func TestDone_WithoutTrackPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	New().Done()
}

func TestStopWithNothingTrackedDrainsImmediately(t *testing.T) {
	term := New()
	term.Stop()
	select {
	case <-term.Drained():
	case <-time.After(time.Second):
		t.Fatal("not drained")
	}
}
