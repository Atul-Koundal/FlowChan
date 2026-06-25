// Package termination provides structured shutdown for concurrent programs.
// It lets you signal that no new work should start while still allowing
// in-flight work to finish cleanly.
package termination

import (
	"context"
	"sync"
)

//raceful shutdown coordinator.
//  When you call Stop(), no new work is accepted.
//  Wait() then blocks until all currently in-flight work finishes before returning.
//  Without this, a naive cancel() would kill goroutines mid-work,
//  potentially leaving data in a corrupt or incomplete state.

// Terminator manages graceful shutdown - signals stop,
// waits for in-flight work to drain before returning.

// Terminator coordinates graceful shutdown.
type Terminator struct {
	mu       sync.Mutex
	stopped  bool
	wg       sync.WaitGroup
	stopCh   chan struct{}
	drainCh  chan struct{}
}

// New creates a ready to use Terminator.
func New() *Terminator {
	return &Terminator{
		stopCh:  make(chan struct{}),
		drainCh: make(chan struct{}),
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
	t.wg.Add(1)
	return true
}

// Done marks one unit of in-flight work as complete.
func (t *Terminator) Done() {
	t.wg.Done()
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
}

// Wait blocks until Stop is called and all tracked goroutines finish.
func (t *Terminator) Wait() {
	<-t.stopCh  // wait for stop signal
	t.wg.Wait() // wait for all in-flight work to finish
}

// Stopped returns a channel that closes when Stop is called.
func (t *Terminator) Stopped() <-chan struct{} {
	return t.stopCh
}

// WithTerminator wraps a context so it cancels when the terminator
// stops and all work drains.
func WithTerminator(ctx context.Context, t *Terminator) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		t.Wait()
		cancel()
	}()
	return ctx, cancel
}