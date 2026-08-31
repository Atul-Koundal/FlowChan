// Package xatomic provides atomic integer wrappers used internally
// by FlowChan packages for metrics tracking.
package xatomic

import "sync/atomic"

// Int64 wraps atomic.Int64 with simpler method names.
type Int64 struct {
	v atomic.Int64
}

// Add atomically adds delta to the value.
func (a *Int64) Add(delta int64) { a.v.Add(delta) }

// Load atomically loads and returns the value.
func (a *Int64) Load() int64 { return a.v.Load() }

// Int32 wraps atomic.Int32 with simpler method names.
type Int32 struct {
	v atomic.Int32
}

// Add atomically adds delta to the value.
func (a *Int32) Add(delta int32) { a.v.Add(delta) }

// Load atomically loads and returns the value.
func (a *Int32) Load() int32 { return a.v.Load() }