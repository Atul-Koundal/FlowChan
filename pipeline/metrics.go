package pipeline

// Metrics holds live counters for a running stage. Safe to read
// from any goroutine while the stage is running.
type Metrics struct {
	processed atomicInt64
	failed    atomicInt64
	active    atomicInt32
}

// NewMetrics returns a fresh, zeroed Metrics collector.
func NewMetrics() *Metrics {
	return &Metrics{}
}

// MetricsSnapshot is a point-in-time copy of the current counter values.
type MetricsSnapshot struct {
	Processed int64 // total items completed (success or failure)
	Failed    int64 // total items that returned an error
	Active    int32 // workers currently processing an item right now
}

// Snapshot takes a point-in-time reading of the metrics. Safe to call
// concurrently while the stage is running.
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		Processed: m.processed.Load(),
		Failed:    m.failed.Load(),
		Active:    m.active.Load(),
	}
}