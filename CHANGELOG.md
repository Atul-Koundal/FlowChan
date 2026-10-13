# Changelog

All notable changes to FlowChan will be documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
FlowChan uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-10-13

### Added

**`pool/`**
- Reusable WorkPool with Start, Submit, Drain, Stop API
- WithRateLimit option to cap tasks per second
- WithMetrics option for live throughput and failure counters
- Panic recovery in safeProcess converts panics to errors
- Rich PoolError type carries task type name and attempt count

**`result/`**
- Renamed from errors/ to avoid shadowing Go standard library
- StageError carries stage name and attempt number
- PoolError carries task type and attempt count
- RetryableError distinguishes transient from permanent failures
- IsRetryable helper for should-retry predicates

**`pipeline/`**
- WithName option sets stage name used in StageError messages
- WithItemTimeout option sets per-item processing deadline
- WithRateLimit option caps throughput across all workers
- WithMetrics option exposes live counters
- Builder and Pipe for chaining more than two stages
- Panic recovery in process converts panics to errors

**`batch/`**
- Flush method triggers immediate flush without waiting for size or timeout

**`stream/`**
- FanOut distributes one stream across N output channels
- FanIn merges N input channels into one output channel
- Split divides a stream into two channels by predicate
- Merge combines multiple streams into one
- WithMetrics option on Map for live counters

**`retry/`**
- ShouldRetryFunc predicate stops retrying on permanent errors
- AlwaysRetry and NeverRetry built-in predicates
- StreamWithDLQ routes permanently failed items to a dead letter queue
- FailedItem carries the original item and last error

**`internal/xatomic/`**
- Shared atomic wrapper types used by pool, pipeline, and stream metrics
- Eliminates duplicated atomic code across packages

### Changed
- errors/ package renamed to result/ to avoid shadowing Go standard library
- pool.NewWorkPool no longer accepts a task slice - use Submit instead
- pool.WorkPool.Run removed - replaced by Start/Submit/Drain/Stop