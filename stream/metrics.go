package stream

import "sync/atomic"

// atomicInt64 wraps atomic.Int64 for use in Metrics.
type atomicInt64 struct {
	v atomic.Int64
}

func (a *atomicInt64) Add(delta int64) { a.v.Add(delta) }
func (a *atomicInt64) Load() int64     { return a.v.Load() }

// atomicInt32 wraps atomic.Int32 for use in Metrics.
type atomicInt32 struct {
	v atomic.Int32
}

func (a *atomicInt32) Add(delta int32) { a.v.Add(delta) }
func (a *atomicInt32) Load() int32     { return a.v.Load() }

// Metrics holds live counters for a running stream operation.
// Safe to read from any goroutine while the stream is running.
type Metrics struct {
	processed atomicInt64
	failed    atomicInt64
	active    atomicInt32
}

// NewMetrics returns a fresh zeroed Metrics collector.
func NewMetrics() *Metrics {
	return &Metrics{}
}

// MetricsSnapshot is a point-in-time copy of the current counters.
type MetricsSnapshot struct {
	Processed int64
	Failed    int64
	Active    int32
}

// Snapshot returns the current counter values.
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		Processed: m.processed.Load(),
		Failed:    m.failed.Load(),
		Active:    m.active.Load(),
	}
}