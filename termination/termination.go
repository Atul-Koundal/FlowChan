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
type Terminator struct {
	mu       sync.Mutex
	stopped  bool
	wg       sync.WaitGroup
	stopCh   chan struct{}
	drainCh  chan struct{}
}

func New() *Terminator {
	return &Terminator{
		stopCh:  make(chan struct{}),
		drainCh: make(chan struct{}),
	}
}

// Track registers one unit of in-flight work.
// Returns false if already stopped - caller should not proceed.
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

// Stop signals shutdown - no new work should start after this.
func (t *Terminator) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.stopped {
		t.stopped = true
		close(t.stopCh)
	}
}

// Wait blocks until Stop is called AND all in-flight work drains.
func (t *Terminator) Wait() {
	<-t.stopCh  // wait for stop signal
	t.wg.Wait() // wait for all in-flight work to finish
}

// Stopped returns a channel that closes when Stop is called.
// Use in select to react to shutdown signal.
func (t *Terminator) Stopped() <-chan struct{} {
	return t.stopCh
}

// WithTerminator wraps a context so it cancels when
// the terminator stops AND all work drains.
func WithTerminator(ctx context.Context, t *Terminator) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		t.Wait()
		cancel()
	}()
	return ctx, cancel
}