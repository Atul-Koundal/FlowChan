# FlowChan Architecture

This document explains how FlowChan is structured, how packages relate to each other, and the design decisions behind the library.

## Overview

FlowChan is built around one idea: **concurrent operations should be composable**. Each package is a self-contained primitive that works standalone and composes cleanly with the others through a shared channel-based contract.

The contract is simple:

```
input <-chan T  →  [operation]  →  output <-chan Result[T]
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
    all import errors/
    none import each other

errors/
    imported by everyone
    imports nothing from FlowChan
```

This means any single package can be used independently. A project that only needs a worker pool imports only `pool/`. A project that only needs retry logic imports only `retry/`. No hidden coupling.

## Package responsibilities

### `errors/` - the shared contract

Every concurrent operation in FlowChan returns `chan Result[T]`. This type is the glue that holds the library together.

```go
type Result[T any] struct {
    Value T
    Err   error
}
```

Without this, every package would need two channels - one for values, one for errors - and keeping them in sync across goroutines is fragile. `Result[T]` keeps them together so errors are never silently dropped.

Built first, never changed. Every other package depends on it.

### `pool/` - heterogeneous task runner

The pool solves a different problem than the pipeline. Where the pipeline transforms uniform data through typed stages, the pool runs **different types of work** concurrently through a common interface.

```
EmailTask, ResizeTask, ReportTask  →  [WorkPool]  →  []error
```

The `Task` interface is the key design:

```go
type Task interface {
    Process() error
}
```

Any struct that implements `Process() error` can run in the same pool. The pool does not care what the work is. This is intentional - it mirrors how real job queues work, where a worker picks up whatever is in the queue regardless of type.

The WaitGroup tracks **workers** not tasks. This was a deliberate design decision made during development. Tracking tasks means `wg.Done()` can only be called after a task completes - if the context cancels mid-flight, `wg.Wait()` hangs forever. Tracking workers means `defer wg.Done()` fires when a goroutine exits for any reason.

### `pipeline/` - typed transformation stages

A Stage is a function `In -> Out` running across N goroutines:

```
<-chan In  →  [Stage: fn runs on N workers]  →  <-chan Result[Out]
```

Stages are generic - `Stage[In, Out any]`. The type system enforces that stages are connected correctly. You cannot chain a `Stage[int, string]` into a `Stage[float64, bool]` - it will not compile.

`Chain()` connects two stages. The key design decision here is error handling between stages: errors from stage 1 are **not dropped**. They bypass stage 2 and flow directly to the final output channel. This required a fan-in merge of two channels - the error channel from stage 1 and the output channel from stage 2.

`OrderedStage` adds a sequence number to each item before processing, then holds completed items in a map buffer, releasing them in order. The buffer can grow if later items complete before earlier ones - this is the memory tradeoff of ordering.

### `batch/` - grouping items

Two batching strategies with different flush triggers:

```
Batcher         - flush on size OR fixed ticker
RealtimeBatcher - flush on size OR sliding window (resets on each new item)
```

The sliding window is the meaningful difference. A fixed ticker fires every N milliseconds regardless of activity. A sliding window fires only after N milliseconds of **silence**. For event streams with bursts, the sliding window produces more natural groupings - the burst is processed together rather than split across ticker boundaries.

Both strategies always flush remaining items when the input channel closes, ensuring no data is lost.

### `stream/` - concurrent transformations

Three variants of the same idea - apply a function to items concurrently:

```
Map         - concurrent, unordered, fastest
OrderedMap  - concurrent, ordered, uses sequence buffer (same as OrderedStage)
FlatMap     - concurrent, 1-to-many expansion, unordered
```

`BackpressureMap` adds a semaphore. A buffered channel of capacity N acts as the semaphore - acquiring a slot blocks when all N are taken, which naturally slows the producer without any explicit signalling.

### `retry/` - resilient operations

Three backoff strategies building on each other:

```
Fixed            - constant wait
Exponential      - doubling wait, capped at max
ExponentialJitter - exponential + random spread to prevent thundering herd
```

Jitter solves a real problem: if 100 workers all fail at the same time and all retry after exactly 2 seconds, they all hit the recovering system simultaneously. Random jitter spreads retries over a window, giving the system time to recover incrementally.

The jitter cap is important: after adding jitter to the exponential value, the result is capped at max again. Without the second cap, jitter can push the wait above the maximum.

`Stream()` wraps any pipeline function with retry behaviour. Each item is retried independently - a failure on item 3 does not affect item 4.

### `termination/` - graceful shutdown

The core insight: `context.Cancel()` is abrupt - it signals goroutines to stop but does not wait for them. `Terminator` adds the waiting layer.

```
term.Stop()  - signal: no new work accepted
term.Wait()  - block: until all tracked goroutines call Done()
```

The `Track() bool` return value is the safety mechanism. If `Stop()` has already been called, `Track()` returns false and the caller must not start new work. This prevents the race where a goroutine calls `Track()` just after `Stop()` and starts work that will never be waited on.

### `iter/` - consumer ergonomics

Iterators are a thin layer over channels. Their job is to let library users write:

```go
results := iter.Collect(
    iter.Map(
        iter.Filter(seq, isEven),
        double,
    ),
)
```

instead of:

```go
var results []int
for {
    item, ok := <-ch
    if !ok {
        break
    }
    if isEven(item) {
        results = append(results, double(item))
    }
}
```

The push iterator pattern (`func(yield func(T) bool)`) enables early exit - returning false from yield stops iteration and allows goroutines to clean up without leaking.

## Core design decisions

**Channels as the API surface, not structs**

Every operation returns a channel. This means operations compose naturally - the output of one is the input of the next. No adapter types, no glue code. This mirrors how Unix pipes work.

**Generics throughout**

Everything is generic (`[T any]`, `[In, Out any]`). The type system catches connection errors at compile time. You cannot accidentally feed `chan int` into a stage expecting `chan string`.

**Context cancellation at every blocking point**

Every `select` that could block has a `ctx.Done()` case. This is non-negotiable for library code. Without it, a cancelled context can leave goroutines blocked forever, leaking memory and goroutines. Every channel send and receive that could block uses a select.

**Workers track themselves, not their work**

WaitGroups track goroutines, not items. `defer wg.Done()` fires when a goroutine exits for any reason - context cancellation, channel close, or normal completion. Tracking items instead leads to the hung-`wg.Wait()` bug discovered during pool development.

**Panic recovery at every worker boundary**

A panic inside user-provided `fn` should not crash the program. Every worker wraps its `fn` call in a `defer recover()` and converts panics into errors. This is a library's responsibility - the library cannot know what the user's function might do.

## Adding a new package

Follow these rules to stay consistent with the existing design:

1. Accept `context.Context` as the first parameter in every public function
2. Accept input as `<-chan T` and return output as `<-chan Result[T]`
3. Close the output channel when input is exhausted or context is done
4. Track goroutines with a WaitGroup, not items
5. Wrap every user-provided function call in a `defer recover()`
6. Add a `ctx.Done()` case to every select that could block
7. Write tests with `-race` before writing anything else
8. Add godoc comments to every exported symbol

## What is not in FlowChan

These are deliberate omissions, not oversights:

**No global state** - every operation takes explicit inputs and returns explicit outputs. Nothing is configured globally.

**No reflection** - everything is done through generics and interfaces. No `interface{}`, no `reflect`.

**No external dependencies** - the library uses only the Go standard library. This keeps it lightweight and avoids dependency conflicts in consuming projects.

**No built-in logging** - logging belongs to the application, not the library. The Metrics type gives applications the data they need to log or instrument however they choose.