package pipeline

import "sync/atomic"

// atomicInt64 wraps atomic.Int64 with simpler method names,
// used internally by Metrics.
type atomicInt64 struct {
	v atomic.Int64
}

func (a *atomicInt64) Add(delta int64) { a.v.Add(delta) }
func (a *atomicInt64) Load() int64     { return a.v.Load() }

// atomicInt32 wraps atomic.Int32 with simpler method names,
// used internally by Metrics.
type atomicInt32 struct {
	v atomic.Int32
}

func (a *atomicInt32) Add(delta int32) { a.v.Add(delta) }
func (a *atomicInt32) Load() int32     { return a.v.Load() }