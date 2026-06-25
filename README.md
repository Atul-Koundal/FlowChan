# FlowChan
FlowChan is a concurrency toolkit for Go. It gives you worker pools, typed pipelines, batching, streaming, retries, and graceful shutdown - so you focus on the logic, not the plumbing.

```bash
go get https://github.com/Atul-Koundal/FlowChan
```

---

## Goals

**Make common concurrency tasks easier.**
FlowChan provides a clean and safe way to solve common concurrency problems - parallel job execution, stream processing, batching, retries. It removes boilerplate and abstracts away the complexity of goroutine lifecycle, channel management, and error propagation. Developers retain full control over the concurrency level of every operation.

**Make concurrent code composable.**
Most functions in the library take Go channels as inputs and return new transformed channels as outputs. This allows them to be chained together to build pipelines from simpler parts, similar to Unix pipes. Concurrent programs become clear sequences of reusable operations.

**Centralize error handling.**
Errors are carried through the pipeline automatically via the `Result[T]` type and can be handled in a single place at the end. Every value and its error travel together - nothing is silently dropped.

**Support heterogeneous work.**
Unlike stream-only libraries, FlowChan includes a task pool that runs different types of work concurrently through a common `Task` interface. Email sending, image resizing, and report generation can all run in the same pool without any shared type.

**Handle real-world failure.**
FlowChan includes retry strategies with fixed, exponential, and exponential-with-jitter backoff built in. Operations that fail temporarily can recover automatically without crashing the pipeline.

**Shut down gracefully.**
The termination package lets you signal shutdown and wait for all in-flight work to drain before the program exits. No goroutines are cut off mid-execution.

**Keep it lightweight.**
FlowChan has zero external dependencies. It operates entirely on standard Go channels and goroutines, making it straightforward to integrate into any existing project.

---

## Quick Start

Let's look at a practical example: fetch users from an API, activate them, and save the changes back. It shows how to control concurrency at each step while keeping the code clean.

```go
func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // convert a slice of IDs into a channel
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

---

## Worker Pool

Run different types of tasks concurrently. Any struct implementing `Process() error` is a task - the pool does not care what the work actually is.

```go
type EmailTask struct{ To, Subject string }
func (t *EmailTask) Process() error {
    return sendEmail(t.To, t.Subject)
}

type ResizeTask struct{ File string; W, H int }
func (t *ResizeTask) Process() error {
    return resizeImage(t.File, t.W, t.H)
}

tasks := []pool.Task{
    &EmailTask{To: "alice@example.com", Subject: "Welcome"},
    &ResizeTask{File: "photo.jpg", W: 800, H: 600},
}

wp := pool.NewWorkPool(tasks, 3)
errs := wp.Run(context.Background())
```

### Retries

Tasks that fail temporarily can be retried automatically before being counted as errors.

```go
wp := pool.NewWorkPool(tasks, 3, pool.WithRetries(3))
errs := wp.Run(ctx)
```

The task runs up to 4 times total (1 original + 3 retries). If it succeeds on any attempt, no error is recorded.

---

## Batching

Processing items in batches rather than individually can significantly improve performance when working with databases or external services. Batching reduces the number of queries and API calls, increases throughput, and typically lowers costs.

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

### Real-Time Batching

Real-world applications often handle events that arrive at unpredictable rates. While batching is still desirable for efficiency, waiting to collect a full batch introduces unacceptable delays when the input stream is slow or sparse.

FlowChan solves this with a sliding window batcher. Batches flush either when they hit the size limit or when no new items arrive for the full window duration, whichever comes first.

```go
// flush when 100 items accumulate OR after 100ms of silence
b := batch.NewRealtime[int](100, 100*time.Millisecond)

for r := range b.Run(ctx, events) {
    db.BulkInsert(r.Value)
}
```

---

## Errors, Termination and Contexts

Error handling in concurrent programs is non-trivial. FlowChan simplifies this with the `Result[T]` type. Every value and its error travel together through the pipeline - errors are never lost or silently dropped.

```go
for r := range out {
    val, err := r.Unwrap()
    if err != nil {
        // handle error
        continue
    }
    // use val
}

// helpers
if r.IsErr() { ... }

// transform without unwrapping
mapped := errors.Map(r, func(n int) string {
    return fmt.Sprintf("id-%d", n)
})

// split a batch of results into values and errors
values, errs := errors.Collect(results)
```

Context cancellation is respected everywhere. Pass a context with a timeout or cancel function and all goroutines exit cleanly.

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

wp := pool.NewWorkPool(tasks, 5)
errs := wp.Run(ctx) // stops cleanly when context times out
```

### Graceful Termination

For cases where you need to stop accepting new work but still need in-flight work to finish, the termination package provides structured shutdown.

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

---

## Order Preservation

Concurrent processing can boost performance, but since tasks take different amounts of time, results usually arrive out of order. While this is acceptable in many scenarios, some cases require preserving the original input order.

`NewOrderedStage` performs additional synchronization under the hood to ensure that if item X precedes item Y in the input, the result of X will precede the result of Y in the output.

```go
stage := pipeline.NewOrderedStage(5, func(ctx context.Context, n int) (string, error) {
    time.Sleep(time.Duration(10-n) * time.Millisecond) // variable processing time
    return fmt.Sprintf("item-%d", n), nil
})

// output is always item-1, item-2, item-3... regardless of completion order
for r := range stage.Run(ctx, in) {
    fmt.Println(r.Value)
}
```

---

## Parallel Streaming and FlatMap

FlatMap transforms each input item into multiple output items, then merges them all into a single stream. It gives full control over the concurrency level, meaning at most N input items are being expanded at the same time.

```go
departments := toChan("IT", "Finance", "Marketing", "Engineering")

// stream users from all departments concurrently
// at most 3 departments processed at the same time
users := stream.FlatMap(ctx, departments, 3,
    func(ctx context.Context, dept string) ([]User, error) {
        return api.GetUsersByDepartment(ctx, dept)
    })

for r := range users {
    fmt.Println(r.Value.Name)
}
```

For cases where order does not matter, `Map` is faster. When order must be preserved, use `OrderedMap`.

```go
// unordered - faster
out := stream.Map(ctx, in, 5, fetchUser)

// ordered - preserves input sequence
out := stream.OrderedMap(ctx, in, 5, fetchUser)
```

### Backpressure

When a fast producer feeds slow workers, unbounded memory growth is a real risk. `BackpressureMap` caps the number of in-flight items at any moment. When all worker slots are full, reading from the input channel blocks naturally, slowing the producer down.

```go
out := stream.BackpressureMap(ctx, in, 3, func(ctx context.Context, n int) (int, error) {
    return process(n) // at most 3 items in-flight at once
})
```

---

## Retry with Backoff

Retrying failed operations is critical for building resilient systems. FlowChan provides three built-in backoff strategies and a stream-level retry wrapper that integrates directly into any pipeline.

```go
// fixed - same wait every time
retry.Fixed(1 * time.Second)

// exponential - doubles every attempt, capped at max
retry.Exponential(1*time.Second, 30*time.Second)

// exponential with jitter - recommended for production
// prevents thundering herd when many workers retry simultaneously
retry.ExponentialJitter(1*time.Second, 30*time.Second)
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

for r := range out {
    if r.IsErr() {
        fmt.Println("failed after all retries:", r.Err)
        continue
    }
    fmt.Println("result:", r.Value)
}
```

---

## Go 1.23 Iterators

FlowChan integrates with Go 1.23 range-over-function iterators. Convert any channel into an iterator and compose it with `Filter`, `Map`, and `Collect` without writing raw channel loops.

```go
seq := iter.FromChan(ctx, numbersChan)

results := iter.Collect(
    iter.Map(
        iter.Filter(seq, func(n int) bool { return n%2 == 0 }),
        func(n int) int { return n * 2 },
    ),
)

fmt.Println(results) // all even numbers, doubled
```

Convert a result channel into an iterator:

```go
seq := iter.FromResults(ctx, resultsChan)
seq(func(val int, err error) bool {
    if err != nil {
        return true // skip errors, continue
    }
    fmt.Println(val)
    return true
})
```

---

## Testing Strategy

All packages are tested with the `-race` flag. Tests are focused on:

- **Correctness** - functions produce accurate results at different concurrency levels
- **Cancellation** - context cancellation exits cleanly without goroutine leaks
- **Error propagation** - errors flow through correctly and are never silently dropped
- **Ordering** - ordered variants preserve input sequence, unordered variants do not
- **Backpressure** - concurrency limits are strictly enforced under load

```bash
# run all tests
go test ./... -race

# run with timeout to catch hangs
go test ./... -race -timeout 30s

# run a specific package
go test ./retry/... -v -race
```

---

## Project Structure

```
flowchan/
├── go.mod
├── pool/               # Task interface, WorkPool, retries
├── errors/             # Result[T] - value + error carrier
├── pipeline/
│   ├── stage.go        # Stage[In,Out], Chain
│   └── ordered.go      # OrderedStage strict output ordering
|   |__ metrics.go      # Throughput,latency,counters
|   |__ atomic.go       # Atomic counters, flags, state
├── batch/
│   ├── batch.go        # fixed size + timeout batching
│   └── realtime.go     # sliding window realtime batching
├── stream/
│   ├── stream.go       # Map, OrderedMap, FlatMap
│   └── backpressure.go # BackpressureMap - concurrency cap
├── termination/        # graceful shutdown, drain in-flight work
├── retry/              # Fixed, Exponential, ExponentialJitter backoff
├── iter/               # Seq iterators, Filter, Map, Collect
└── example/            # end-to-end usage of all packages
```

Packages only depend downward. Nothing in `pool` imports `pipeline`. `errors` imports nothing from FlowChan. Any package can be used independently without pulling in the rest of the library.

## Acknowledgements

Thanks to [Rill](https://github.com/destel/rill) for being an amazing read and a source of inspiration for some of the concurrency patterns used in this project.

---

## License

MIT
