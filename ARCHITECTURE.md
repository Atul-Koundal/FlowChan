# FlowChan Architecture

This document explains how FlowChan is structured, how packages relate to each other, and the design decisions behind the library.

## Overview

FlowChan is built around one idea: concurrent operations should be composable. Each package is a self-contained primitive that works standalone and composes cleanly with the others through a shared channel-based contract.

The contract is simple:

```
input <-chan T  ->  [operation]  ->  output <-chan Result[T]
```

Every operation reads from a channel, does work concurrently, and writes results to a new channel. Channels close when work is done. Context cancellation is respected everywhere.

## Package dependency graph

Packages only depend downward. Nothing in an upper layer imports a lower layer's sibling.

```
example/
    imports everything - shows end-to-end usage

iter/
    sits on top of all packages
    converts channel output into composable iterators

pool/      pipeline/     batch/      stream/     retry/     termination/
    all import result/
    pool, pipeline, stream import internal/xatomic via their metrics files
    none import each other

result/
    imported by everyone
    imports nothing from FlowChan

internal/xatomic/
    imported by pool, pipeline, stream
    imports nothing from FlowChan
```

This means any single package can be used independently. A project that only needs a worker pool imports only pool/. A project that only needs retry logic imports only retry/.

## Package responsibilities

### `result/` - the shared contract

Every concurrent operation in FlowChan returns chan Result[T]. This type is the glue that holds the library together.

```go
type Result[T any] struct {
    Value T
    Err   error
}
```

Without this, every package would need two channels - one for values, one for errors - and keeping them in sync across goroutines is fragile. Result[T] keeps them together so errors are never silently dropped.

The package also defines rich error types that carry context about failures:

```go
type StageError struct {
    Stage   string
    Attempt int
    Err     error
}

type PoolError struct {
    TaskType string
    Attempts int
    Err      error
}

type RetryableError struct {
    Err error
}
```

These wrap underlying errors while remaining compatible with errors.Is and errors.As. Callers who do not care about the extra context see a normal error. Callers who want to know which stage failed or how many retries were attempted can unwrap it.

### `internal/xatomic/` - shared atomic primitives

Three packages (pool, pipeline, stream) all need atomic integer counters for metrics. The naive approach copies the same wrapper types into each package. xatomic defines them once:

```go
type Int64 struct{ v atomic.Int64 }
type Int32 struct{ v atomic.Int32 }
```

The internal/ path means nothing outside this module can import it. It is a shared implementation detail, not a public API.

### `pool/` - heterogeneous task runner

The pool solves a different problem than the pipeline. Where the pipeline transforms uniform data through typed stages, the pool runs different types of work concurrently through a common interface.

```go
type Task interface {
    Process() error
}
```

Any struct that implements Process() error can run in the same pool. This is intentional - it mirrors how real job queues work, where a worker picks up whatever is in the queue regardless of type.

The reusable design (v0.2.0 change) uses Start/Submit/Drain/Stop instead of a one-shot Run:

```
Start()   - launch workers, call once
Submit()  - send work, call as many times as needed
Drain()   - wait for current queue to empty
Stop()    - shut down workers, collect errors
```

This matches how a long-running service actually uses a pool - workers run continuously, work arrives continuously, shutdown happens once.

The WaitGroup tracks workers not tasks. This was a deliberate design decision. Tracking tasks means wg.Done() can only be called after a task completes. If context cancels mid-flight, wg.Wait() hangs forever waiting for tasks that will never complete. Tracking workers means defer wg.Done() fires when a goroutine exits for any reason.

Rate limiting happens at Submit() not inside workers. If multiple workers all wait on the same ticker channel, several can fire on the same tick simultaneously, defeating the rate limit. Moving the wait to Submit() serialises it correctly.

### `pipeline/` - typed transformation stages

A Stage is a function In -> Out running across N goroutines:

```
<-chan In  ->  [Stage: fn runs on N workers]  ->  <-chan Result[Out]
```

Stages are generic - Stage[In, Out any]. The type system enforces that stages connect correctly at compile time.

Chain() connects two stages. The key design decision is error handling between stages: errors from stage 1 bypass stage 2 and flow directly to the final output. This required splitting the mid-results channel into two paths and merging them at the end.

Builder and Pipe extend this to N stages without nesting:

```go
b := pipeline.Pipe(pipeline.Pipe(pipeline.NewBuilder(s1), s2), s3)
```

Each Pipe call wraps the previous builder, threading errors through at every level.

Per-item timeout wraps the context passed to fn with a deadline. If fn does not respect context cancellation the timeout has no effect, but well-written functions using standard library HTTP clients, database drivers, and gRPC stubs all respect context automatically.

### `batch/` - grouping items

Two batching strategies:

```
Batcher         - flush on size OR fixed ticker
RealtimeBatcher - flush on size OR sliding window (resets on each new item)
```

The sliding window is the meaningful difference. A fixed ticker fires every N milliseconds regardless of activity. A sliding window fires only after N milliseconds of silence. For event streams with bursts, the sliding window produces more natural groupings.

Manual flush sends a signal through a buffered channel of capacity 1. The select default case means Flush() never blocks, and a second Flush() before the first is consumed has no additional effect.

### `stream/` - concurrent transformations

Four variants of apply-function-to-items:

```
Map             - concurrent, unordered, fastest
OrderedMap      - concurrent, ordered, sequence buffer overhead
FlatMap         - concurrent, 1-to-many expansion, unordered
BackpressureMap - concurrent, semaphore-capped in-flight items
```

OrderedMap tags each item with a sequence number before processing, collects results into a map, and releases them in order. The buffer can grow if later items finish before earlier ones.

BackpressureMap uses a buffered channel as a semaphore. Acquiring a slot blocks when all N are taken, which slows the producer naturally without any explicit signalling.

FanOut distributes items round-robin across N output channels. Each item goes to exactly one output.

FanIn merges N input channels into one output using one goroutine per input, all writing to a shared output with a WaitGroup closing it when all inputs drain.

Split routes items to one of two outputs based on a predicate. Errors always go to the first output to avoid silently dropping them.

Merge is structurally identical to FanIn but operates on Result channels directly.

### `retry/` - resilient operations

Three backoff strategies building on each other:

```
Fixed            - constant wait
Exponential      - doubling wait, capped at max
ExponentialJitter - exponential + random spread
```

Jitter solves the thundering herd problem. When many workers fail simultaneously and retry at the exact same instant, they overwhelm a recovering system. Jitter spreads retries across a window. The jitter cap is applied twice: once to cap the exponential, once after adding jitter, because adding jitter to the capped value can push it over the maximum again.

ShouldRetryFunc lets callers distinguish transient errors from permanent ones. A network timeout is transient. An authentication failure is permanent. Without this distinction, retrying a permanent error wastes time and delays reporting.

StreamWithDLQ separates permanently failed items from the main output. Both channels must be consumed concurrently - the single goroutine writes to both and blocks if either is full.

### `termination/` - graceful shutdown

context.Cancel() is abrupt. It signals goroutines to stop but does not wait. Terminator adds the waiting layer.

The Track() bool return value is the safety mechanism. If Stop() has already been called, Track() returns false and the caller must not start new work. This prevents the race where a goroutine calls Track() just after Stop() is called.

### `iter/` - consumer ergonomics

Iterators are a thin layer over channels. Their job is composability. The push iterator pattern (func(yield func(T) bool)) enables early exit: returning false from yield stops iteration and allows goroutines to clean up without leaking.

## Core design decisions

**Channels as the API surface**
Every operation returns a channel. Operations compose naturally. The output of one is the input of the next with no adapter types.

**Generics throughout**
Everything is generic. Type errors are compile-time not runtime. You cannot feed chan int into a stage expecting chan string.

**Context cancellation at every blocking point**
Every select that could block has a ctx.Done() case. Without it, a cancelled context leaves goroutines blocked forever.

**Workers track themselves not their work**
WaitGroups track goroutines. defer wg.Done() fires on any exit reason. Tracking items leads to hung wg.Wait() when context cancels.

**Panic recovery at every worker boundary**
A panic inside user-provided fn should not crash the program. Every worker wraps its fn call in defer recover() and converts panics to errors.

**Rich errors without breaking callers**
StageError and PoolError wrap underlying errors. Callers using errors.Is and errors.As get full context. Callers who just print the error get a readable message. Nothing breaks.

## Adding a new package

1. Accept context.Context as the first parameter in every public function
2. Accept input as <-chan T and return output as <-chan Result[T]
3. Close the output channel when input is exhausted or context is done
4. Track goroutines with a WaitGroup not items
5. Wrap every user-provided function call in defer recover()
6. Add a ctx.Done() case to every select that could block
7. Write tests with -race and goleak before writing anything else
8. Add godoc comments to every exported symbol
9. Import nothing from FlowChan except result/ and internal/xatomic/

## What is not in FlowChan

**No global state** - every operation takes explicit inputs and returns explicit outputs.

**No reflection** - everything is done through generics and interfaces.

**No external dependencies** - only the Go standard library. goleak is a test-only dependency.

**No built-in logging** - logging belongs to the application. Metrics gives applications the data they need to log however they choose.

**No persistence** - FlowChan operates entirely in memory. Dead letter queues are channels, not databases.