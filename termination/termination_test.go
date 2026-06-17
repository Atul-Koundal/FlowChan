package termination

import (
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestTerminator_DrainBeforeExit(t *testing.T) {
	term := New()

	var completed atomic.Int32

	// start 5 in-flight workers
	for i := 0; i < 5; i++ {
		if !term.Track() {
			t.Fatal("Track returned false before Stop was called")
		}
		go func() {
			defer term.Done()
			time.Sleep(100 * time.Millisecond)
			completed.Add(1)
		}()
	}

	// stop in background
	go func() {
		time.Sleep(50 * time.Millisecond)
		term.Stop()
	}()

	term.Wait()

	if completed.Load() != 5 {
		t.Errorf("expected 5 completed, got %d", completed.Load())
	}
}

func TestTerminator_NoNewWorkAfterStop(t *testing.T) {
	term := New()
	term.Stop()

	accepted := term.Track()
	if accepted {
		t.Error("Track should return false after Stop")
	}
}

func TestTerminator_StopIdempotent(t *testing.T) {
	term := New()
	// calling Stop multiple times should not panic
	term.Stop()
	term.Stop()
	term.Stop()
}

func TestTerminator_WaitUnblocksAfterDrain(t *testing.T) {
	term := New()

	if !term.Track() {
		t.Fatal("unexpected Track failure")
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		term.Done()
		term.Stop()
	}()

	done := make(chan struct{})
	go func() {
		term.Wait()
		close(done)
	}()

	select {
	case <-done:
		// good
	case <-time.After(1 * time.Second):
		t.Fatal("Wait did not unblock after drain")
	}
}