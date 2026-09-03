# FlowChan

FlowChan is a concurrency toolkit for Go. It gives you worker pools, typed pipelines, batching, streaming, retries, and graceful shutdown so you focus on the logic, not the plumbing.

```bash
go get github.com/Atul-Koundal/FlowChan
```

## Goals

**Make common concurrency tasks easier.**
FlowChan provides a clean and safe way to solve common concurrency problems: parallel job execution, stream processing, batching, retries. It removes boilerplate and abstracts away the complexity of goroutine lifecycle, channel management, and error propagation. Developers retain full control over the concurrency level of every operation.

**Make concurrent code composable.**
Most functions in the library take Go channels as inputs and return new transformed channels as outputs. This allows them to be chained together to build pipelines from simpler parts, similar to Unix pipes. Concurrent programs become clear sequences of reusable operations.

**Centralize error handling.**
Errors are carried through the pipeline automatically via the `Result[T]` type and can be handled in a single place at the end. Every value and its error travel together, nothing is silently dropped.

**Support heterogeneous work.**
Unlike stream-only libraries, FlowChan includes a task pool that runs different types of work concurrently through a common `Task` interface. Email sending, image resizing, and report generation can all run in the same pool without any shared type.

**Handle real-world failure.**
FlowChan includes retry strategies with fixed, exponential, and exponential-with-jitter backoff built in. Operations that fail temporarily can recover automatically without crashing the pipeline. A should-retry predicate lets callers distinguish permanent errors from transient ones.

**Shut down gracefully.**
The termination package lets you signal shutdown and wait for all in-flight work to drain before the program exits. No goroutines are cut off mid-execution.

**Keep it lightweight.**
FlowChan has zero external dependencies. It operates entirely on standard Go channels and goroutines, making it straightforward to integrate into any existing project.

## Quick Start

Fetch users from an API, activate them, and save the changes back. Each step runs concurrently with independent worker counts.

```go
func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    ids := make(chan int, 10)
    go func() {
        defer close(ids)
        for i := 1; i <= 10; i++ {
            ids <- i
        }
    }()

    // fetch users concurrently - 3 workers
    stage1 := pipeline.NewStage(3, func(ctx context.Context, id int) (*User, error) {
        return api.GetUser(ctx, id)
    })

    // activate users concurrently - 2 workers
    stage2 := pipeline.NewStage(2, func(ctx context.Context, u *User) (*User, error) {
        u.IsActive = true
        return u, api.SaveUser(ctx, u)
    })

    p := pipeline.Chain(stage1, stage2)
    for r := range p.Run(ctx, ids) {
        if r.IsErr() {
            fmt.Println("error:", r.Err)
            continue
        }
        fmt.Println("saved user:", r.Value.ID)
    }
}
```

## Worker Pool

Run different types of tasks concurrently. Any struct implementing `Process() error` is a task. The pool does not care what the work actually is.

```go
type EmailTask struct{ To, Subject string }

func (t *EmailTask) Process() error {
    return sendEmail(t.To, t.Subject)
}

type ResizeTask struct {
    File string
    W, H int
}

func (t *ResizeTask) Process() error {
    return resizeImage(t.File, t.W, t.H)
}

wp := pool.NewWorkPool(3)
wp.Start(ctx)

wp.Submit(&EmailTask{To: "alice@example.com", Subject: "Welcome"})
wp.Submit(&ResizeTask{File: "photo.jpg", W: 800, H: 600})

wp.Drain()
errs := wp.Stop()
```

### Retries

Tasks that fail temporarily are retried automatically before being counted as errors.

```go
wp := pool.NewWorkPool(3, pool.WithRetries(3))
```

The task runs up to 4 times total (1 original + 3 retries). If it succeeds on any attempt, no error is recorded.

### Pool Metrics

Attach a metrics collector to observe throughput and failures in real time.

```go
m := pool.NewMetrics()
wp := pool.NewWorkPool(5, pool.WithMetrics(m))
wp.Start(ctx)

go func() {
    for range time.Tick(time.Second) {
        snap := m.Snapshot()
        fmt.Printf("processed=%d failed=%d active=%d\n",
            snap.Processed, snap.Failed, snap.Active)
    }
}()
```

### Rate Limiting

Cap how many tasks are accepted per second across all workers.

```go
wp := pool.NewWorkPool(5, pool.WithRateLimit(10)) // max 10 tasks/sec
```

## Batching

Group items into batches before processing. Reduces database queries and API calls significantly.

```go
b := batch.New[int](5, 500*time.Millisecond)

for r := range b.Run(ctx, ids) {
    if r.IsErr() {
        fmt.Println("error:", r.Err)
        continue
    }
    db.BulkInsert(r.Value) // [1 2 3 4 5], [6 7 8 9 10]...
}
```

### Manual Flush

Trigger an immediate flush on demand without waiting for size or timeout.

```go
b := batch.New[int](100, 10*time.Second)
out := b.Run(ctx, in)

b.Flush() // send current batch immediately
```

### Real-Time Batching

A sliding window batcher that only flushes after a period of silence. Useful for event streams where bursts should be processed together.

```go
// flush when 100 items accumulate OR after 100ms of silence
b := batch.NewRealtime[int](100, 100*time.Millisecond)

for r := range b.Run(ctx, events) {
    db.BulkInsert(r.Value)
}
```

## Errors, Termination and Contexts

Every value and its error travel together through the pipeline via `Result[T]`. Errors are never lost or silently dropped.

```go
for r := range out {
    val, err := r.Unwrap()
    if err != nil {
        continue
    }
    fmt.Println(val)
}

// check without unwrapping
if r.IsErr() { ... }

// transform without unwrapping
mapped := result.Map(r, func(n int) string {
    return fmt.Sprintf("id-%d", n)
})

// split a batch of results into values and errors
values, errs := result.Collect(results)
```

Context cancellation is respected everywhere. All goroutines exit cleanly when the context is cancelled.

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

wp := pool.NewWorkPool(5)
wp.Start(ctx)
```

### Rich Error Types

Errors from pipeline stages and pool tasks carry context about where and how they failed.

```go
var stageErr *result.StageError
if errors.As(r.Err, &stageErr) {
    fmt.Printf("stage %q failed on attempt %d: %v\n",
        stageErr.Stage, stageErr.Attempt, stageErr.Err)
}

var poolErr *result.PoolError
if errors.As(err, &poolErr) {
    fmt.Printf("task %q failed after %d attempts: %v\n",
        poolErr.TaskType, poolErr.Attempts, poolErr.Err)
}
```

### Graceful Termination

Signal shutdown and wait for all in-flight work to drain before exiting.

```go
term := termination.New()

for i := 0; i < 5; i++ {
    if !term.Track() {
        continue // already stopped
    }
    go func() {
        defer term.Done()
        doWork()
    }()
}

term.Stop() // no new work accepted after this
term.Wait() // blocks until all in-flight goroutines finish
```

## Order Preservation

`NewOrderedStage` guarantees that if item X precedes item Y in the input, the result of X precedes the result of Y in the output, regardless of processing time.

```go
stage := pipeline.NewOrderedStage(5, func(ctx context.Context, n int) (string, error) {
    time.Sleep(time.Duration(10-n) * time.Millisecond)
    return fmt.Sprintf("item-%d", n), nil
})

for r := range stage.Run(ctx, in) {
    fmt.Println(r.Value) // always item-1, item-2, item-3...
}
```

## Parallel Streaming and FlatMap

FlatMap expands one item into many, merging all results into a single stream.

```go
users := stream.FlatMap(ctx, departments, 3,
    func(ctx context.Context, dept string) ([]User, error) {
        return api.GetUsersByDepartment(ctx, dept)
    })
```

Use `Map` when order does not matter. Use `OrderedMap` when it does.

```go
out := stream.Map(ctx, in, 5, fetchUser)         // unordered, faster
out := stream.OrderedMap(ctx, in, 5, fetchUser)  // ordered
```

### Fan-out and Fan-in

Distribute work across independent consumers and merge results back.

```go
// split one stream into 3 independent output channels
outputs := stream.FanOut(ctx, in, 3)

// merge multiple streams into one
merged := stream.FanIn(ctx, outputs...)
```

### Split and Merge

Route items to different channels based on a predicate, or combine multiple streams.

```go
// split by predicate
evens, odds := stream.Split(ctx, in, func(n int) bool {
    return n%2 == 0
})

// combine multiple streams
out := stream.Merge(ctx, stream1, stream2, stream3)
```

### Backpressure

Cap in-flight items to prevent a fast producer from overwhelming slow workers.

```go
out := stream.BackpressureMap(ctx, in, 3, func(ctx context.Context, n int) (int, error) {
    return process(n) // at most 3 items in-flight at once
})
```

### Stream Metrics

Observe throughput and failures on any stream operation.

```go
m := stream.NewMetrics()
out := stream.Map(ctx, in, 5, fn, stream.WithMetrics(m))

snap := m.Snapshot()
fmt.Println(snap.Processed, snap.Failed, snap.Active)
```

## Retry with Backoff

Three built-in backoff strategies for resilient operations.

```go
retry.Fixed(1 * time.Second)
retry.Exponential(1*time.Second, 30*time.Second)
retry.ExponentialJitter(1*time.Second, 30*time.Second) // recommended for production
```

Use `Do` for a single operation:

```go
err := retry.Do(ctx, 3, retry.ExponentialJitter(time.Second, 30*time.Second), func() error {
    return callAPI()
})
```

Use `Stream` to add retry behaviour to any pipeline stage:

```go
out := retry.Stream(ctx, in, 4,
    retry.ExponentialJitter(10*time.Millisecond, 1*time.Second),
    func(ctx context.Context, item int) (int, error) {
        return process(item)
    })
```

### Should-Retry Predicate

Stop retrying immediately on permanent errors without exhausting all attempts.

```go
var ErrPermanent = errors.New("permanent")

out := retry.Stream(ctx, in, 5, retry.ExponentialJitter(time.Second, 30*time.Second),
    func(ctx context.Context, item int) (int, error) {
        return process(item)
    },
    func(err error) bool {
        return !errors.Is(err, ErrPermanent) // only retry non-permanent errors
    },
)
```

### Dead Letter Queue

Route permanently failed items to a separate channel for inspection or reprocessing.

```go
out, dlq := retry.StreamWithDLQ(ctx, in, 3, retry.ExponentialJitter(time.Second, 30*time.Second), fn)

go func() {
    for failed := range dlq {
        log.Printf("item %v failed permanently: %v", failed.Item, failed.Err)
    }
}()

for r := range out {
    fmt.Println(r.Value)
}
```

## Pipeline Builder

Chain more than two stages together with correct error propagation at every step.

```go
stage1 := pipeline.NewStage(3, parseRaw, pipeline.WithName[string, int]("parse"))
stage2 := pipeline.NewStage(3, validate, pipeline.WithName[int, int]("validate"))
stage3 := pipeline.NewStage(3, store,    pipeline.WithName[int, string]("store"))

b := pipeline.Pipe(pipeline.Pipe(pipeline.NewBuilder(stage1), stage2), stage3)

for r := range b.Run(ctx, input) {
    if r.IsErr() {
        fmt.Println(r.Err) // includes stage name from StageError
        continue
    }
    fmt.Println(r.Value)
}
```

### Per-Item Timeout

Set a deadline on individual items without cancelling the whole pipeline.

```go
stage := pipeline.NewStage(5, fetchUser,
    pipeline.WithItemTimeout[int, *User](3*time.Second),
)
```

### Pipeline Metrics

Observe a running stage from a separate goroutine.

```go
m := pipeline.NewMetrics()
stage := pipeline.NewStage(5, fn, pipeline.WithMetrics[int, int](m))

go func() {
    for range time.Tick(time.Second) {
        snap := m.Snapshot()
        fmt.Printf("processed=%d failed=%d active=%d\n",
            snap.Processed, snap.Failed, snap.Active)
    }
}()
```

## Go 1.23 Iterators

Convert any channel into a composable iterator.

```go
seq := iter.FromChan(ctx, numbersChan)

results := iter.Collect(
    iter.Map(
        iter.Filter(seq, func(n int) bool { return n%2 == 0 }),
        func(n int) int { return n * 2 },
    ),
)
```

## Testing Strategy

All packages are tested with the `-race` flag and goroutine leak detection via `goleak`.

```bash
go test ./... -race -timeout 60s
go test ./benchmarks/... -bench=. -benchmem -count=3
```

Tests cover correctness, cancellation, error propagation, ordering, backpressure, and concurrency limits.

## Project Structure

```
flowchan/
├── go.mod
├── pool/               # Task interface, reusable WorkPool, retries, metrics
├── result/             # Result[T], StageError, PoolError, RetryableError
├── pipeline/
│   ├── stage.go        # Stage[In,Out], Chain, WithName, WithItemTimeout
│   ├── ordered.go      # OrderedStage - strict output ordering
│   ├── builder.go      # Pipe - chain more than two stages
│   └── metrics.go      # live throughput and failure counters
├── batch/
│   ├── batch.go        # fixed size + timeout batching, manual flush
│   └── realtime.go     # sliding window realtime batching
├── stream/
│   ├── stream.go       # Map, OrderedMap, FlatMap, WithMetrics
│   ├── fanout.go       # FanOut, FanIn
│   ├── split.go        # Split, Merge
│   └── backpressure.go # BackpressureMap
├── retry/
│   ├── retry.go        # Fixed, Exponential, ExponentialJitter, Do, Stream
│   └── dlq.go          # StreamWithDLQ - dead letter queue
├── termination/        # graceful shutdown, drain in-flight work
├── iter/               # Seq iterators, Filter, Map, Collect
├── internal/
│   └── xatomic/        # shared atomic wrappers
└── example/            # end-to-end usage of all packages
```

Packages only depend downward. Nothing in `pool` imports `pipeline`. `result` imports nothing from FlowChan. Any package can be used independently.

## Acknowledgements

Thanks to [Rill](https://github.com/destel/rill) for being an inspiring read and a source of ideas for some of the concurrency patterns used in this project.

## License

MIT