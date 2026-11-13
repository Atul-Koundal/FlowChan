// Package termination provides structured shutdown for concurrent programs.
// It lets you signal that no new work should start while still allowing
// in-flight work to finish cleanly, with an optional deadline so a stuck
// goroutine cannot hold shutdown hostage.
package termination

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// Terminator coordinates graceful shutdown. When Stop is called no new
// work is accepted; Wait then blocks until all in-flight work finishes.
// Without it, a plain cancel() would kill goroutines mid-work and could
// leave data in an incomplete state.
type Terminator struct {
	mu         sync.Mutex
	stopped    bool
	inflight   int
	stopCh     chan struct{} // closed by Stop
	drainedCh  chan struct{} // closed once stopped and inflight == 0
	drainedSet bool
}

// New creates a ready to use Terminator.
func New() *Terminator {
	return &Terminator{
		stopCh:    make(chan struct{}),
		drainedCh: make(chan struct{}),
	}
}

// Track registers one unit of in-flight work. Returns false if Stop
// has already been called - caller must not start work in that case.
func (t *Terminator) Track() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return false
	}
	t.inflight++
	return true
}

// Done marks one unit of in-flight work as complete. It panics if called
// more times than Track succeeded.
func (t *Terminator) Done() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.inflight == 0 {
		panic("termination: Done called without a matching Track")
	}
	t.inflight--
	t.closeDrainedLocked()
}

// Stop signals that no new work should be accepted. Safe to call
// multiple times.
func (t *Terminator) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.stopped {
		t.stopped = true
		close(t.stopCh)
	}
	t.closeDrainedLocked()
}

func (t *Terminator) closeDrainedLocked() {
	if t.stopped && t.inflight == 0 && !t.drainedSet {
		t.drainedSet = true
		close(t.drainedCh)
	}
}

// Wait blocks until Stop is called and all tracked work has finished.
// It has no deadline: prefer WaitContext in production.
func (t *Terminator) Wait() {
	<-t.stopCh
	<-t.drainedCh
}

// WaitContext is Wait with a deadline. It returns nil once Stop has been
// called and all tracked work has finished, or ctx.Err() if ctx ends
// first, e.g.
//
//	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
//	defer cancel()
//	if err := term.WaitContext(ctx); err != nil { /* force exit */ }
func (t *Terminator) WaitContext(ctx context.Context) error {
	select {
	case <-t.drainedCh: // implies stopped
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stopped returns a channel that closes when Stop is called.
func (t *Terminator) Stopped() <-chan struct{} {
	return t.stopCh
}

// Drained returns a channel that closes once Stop has been called and
// all tracked work has finished.
func (t *Terminator) Drained() <-chan struct{} {
	return t.drainedCh
}

// WithTerminator returns a context that is cancelled once the terminator
// has stopped and drained. The returned cancel function must be called
// to release resources; calling it (or cancelling parent) also ends
// the internal goroutine, so it never leaks.
func WithTerminator(ctx context.Context, t *Terminator) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-t.drainedCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// StopOnSignal calls t.Stop when the process receives one of sigs
// (SIGINT and SIGTERM if none are given). Call the returned function to
// stop listening; it is safe to call more than once.
func StopOnSignal(t *Terminator, sigs ...os.Signal) (release func()) {
	if len(sigs) == 0 {
		sigs = []os.Signal{os.Interrupt, syscall.SIGTERM}
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, sigs...)
	quit := make(chan struct{})
	go func() {
		select {
		case <-ch:
			t.Stop()
		case <-quit:
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			signal.Stop(ch)
			close(quit)
		})
	}
}
