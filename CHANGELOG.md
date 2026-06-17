# Changelog

All notable changes to FlowChan will be documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
FlowChan uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-06-17

### Added

**`pool/`**
- `Task` interface for heterogeneous concurrent work
- `WorkPool` runs any mix of task types across N workers
- `WithRetries` option for automatic retry on failure
- Panic recovery converts panics to errors instead of crashing

**`errors/`**
- `Result[T]` carries a value and error together through channels
- `Map` transforms a result value without unwrapping
- `Collect` splits a slice of results into values and errors

**`pipeline/`**
- `Stage[In, Out]` generic concurrent transformation stage
- `NewOrderedStage` preserves input order in output
- `Chain` connects two stages with correct error propagation
- `WithRateLimit` caps throughput across all workers
- `WithMetrics` exposes live counters for processed, failed, and active items
- Panic recovery inside stage workers

**`batch/`**
- `Batcher` flushes on size limit or timeout
- `RealtimeBatcher` flushes on size limit or sliding window silence

**`stream/`**
- `Map` concurrent unordered transformation
- `OrderedMap` concurrent transformation preserving input order
- `FlatMap` expands one item into many
- `BackpressureMap` caps in-flight items with a semaphore

**`retry/`**
- `Fixed` backoff strategy
- `Exponential` backoff strategy with configurable cap
- `ExponentialJitter` backoff with jitter to prevent thundering herd
- `Do` retries a single operation
- `DoWithResult` retries an operation that returns a value
- `Stream` adds retry behaviour to any pipeline stage

**`termination/`**
- `Terminator` coordinates graceful shutdown
- `Track` / `Done` register and complete in-flight work
- `Stop` signals no new work accepted
- `Wait` blocks until all in-flight work drains

**`iter/`**
- `Seq[T]` and `SeqErr[T]` push iterator types
- `FromChan` converts a channel to an iterator
- `FromResults` converts a Result channel to a SeqErr iterator
- `ToChan` converts an iterator back to a channel
- `Map` transforms iterator items
- `Filter` keeps items matching a predicate
- `Collect` drains an iterator into a slice