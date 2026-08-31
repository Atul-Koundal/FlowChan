package pipeline

import "github.com/Atul-Koundal/FlowChan/internal/xatomic"

// Metrics holds live counters for a running stage.
// Safe to read from any goroutine while the stage is running.
type Metrics struct {
	processed xatomic.Int64
	failed    xatomic.Int64
	active    xatomic.Int32
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

// Snapshot returns the current counter values. Safe to call
// concurrently while the stage is running.
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		Processed: m.processed.Load(),
		Failed:    m.failed.Load(),
		Active:    m.active.Load(),
	}
}