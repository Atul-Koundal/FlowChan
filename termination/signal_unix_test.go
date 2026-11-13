//go:build unix

package termination

import (
	"syscall"
	"testing"
	"time"
)

func TestStopOnSignal_StopsTerminator(t *testing.T) {
	term := New()
	release := StopOnSignal(term, syscall.SIGUSR1)
	defer release()
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR1); err != nil {
		t.Skip("cannot signal self:", err)
	}
	select {
	case <-term.Stopped():
	case <-time.After(2 * time.Second):
		t.Fatal("terminator not stopped by signal")
	}
	release() // idempotent
}

func TestStopOnSignal_ReleaseLeavesNoGoroutine(t *testing.T) {
	StopOnSignal(New())()
}
