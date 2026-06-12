# FlowChan

A concurrent programming library for Go. FlowChan gives you worker pools, pipelines, batching, streaming, and iterators - so you write the logic, not the boilerplate.

```bash
go get github.com/Atul-Koundal/FlowChan
```

---

## Why FlowChan

Writing concurrent Go by hand means repeating the same patterns every time - goroutine lifecycle, WaitGroups, channel fan-out, error propagation, context cancellation. FlowChan wraps all of that so your code looks like this:

```go
wp := pool.NewWorkPool(tasks, 3)
errs := wp.Run(ctx)
```

instead of this:

```go
var wg sync.WaitGroup
jobs := make(chan Task, len(tasks))
errs := make(chan error, len(tasks))
for i := 0; i < 3; i++ {
    wg.Add(1)
    go func() {
        defer wg.Done()
        for task := range jobs {
            if err := task.Process(); err != nil {
                errs <- err
            }
        }
    }()
}
// ... and so on
```

---

## Packages

| Package | What it does |
|---|---|
| `pool` | Run different types of tasks concurrently |
| `errors` | Carry values and errors safely across goroutines |
| `pipeline` | Chain transformation stages together |
| `batch` | Group items by size or timeout |
| `stream` | FlatMap, parallel map, ordered map |
| `iter` | Consume results with filters, maps, and collectors |
|`termination`| Graceful shutdown/drain in-flight work before stopping |
--- 

## Quick start

### Worker pool

Run multiple types of work concurrently. Any struct with a `Process() error` method is a task.

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/yourname/flowchan/pool"
)

type EmailTask struct {
    To      string
    Subject string
}

func (t *EmailTask) Process() error {
    fmt.Printf("sending email to %s\n", t.To)
    time.Sleep(100 * time.Millisecond)
    return nil
}

type ResizeTask struct {
    Filename string
    Width    int
    Height   int
}

func (t *ResizeTask) Process() error {
    fmt.Printf("resizing %s to %dx%d\n", t.Filename, t.Width, t.Height)
    return nil
}

func main() {
    tasks := []pool.Task{
        &EmailTask{To: "alice@example.com", Subject: "Welcome"},
        &EmailTask{To: "bob@example.com", Subject: "Update"},
        &ResizeTask{Filename: "photo.jpg", Width: 800, Height: 600},
    }

    wp := pool.NewWorkPool(tasks, 3) // 3 concurrent workers
    errs := wp.Run(context.Background())

    for _, err := range errs {
        fmt.Println("error:", err)
    }
}
```

### Pipeline

Chain stages together - the output of one stage becomes the input of the next. Each stage runs concurrently with its own worker count.

```go
// stage 1: parse raw strings into numbers
stage1 := pipeline.NewStage(3, func(ctx context.Context, s string) (int, error) {
    return strconv.Atoi(s)
})

// stage 2: double the number
stage2 := pipeline.NewStage(3, func(ctx context.Context, n int) (int, error) {
    return n * 2, nil
})

in := make(chan string, 5)
go func() {
    defer close(in)
    for _, s := range []string{"1", "2", "3", "4", "5"} {
        in <- s
    }
}()

p := pipeline.Chain(stage1, stage2)
for r := range p.Run(ctx, in) {
    if r.IsErr() {
        fmt.Println("error:", r.Err)
        continue
    }
    fmt.Println("result:", r.Value)
}
```

### Ordered pipeline

When output order must match input order, use NewOrderedStage. Items are processed concurrently but held in a buffer and released in the original sequence.

```go
gostage := pipeline.NewOrderedStage(5, func(ctx context.Context, n int) (string, error) {
    return fmt.Sprintf("item-%d", n*10), nil
})

for r := range stage.Run(ctx, in) {
    fmt.Println(r.Value) // always item-10, item-20, item-30...
}
```

### Batching

Group items into batches before processing. Flushes when the batch hits `size` items or `timeout` elapses - whichever comes first.

```go
b := batch.New[int](3, 500*time.Millisecond)

in := make(chan int, 10)
go func() {
    defer close(in)
    for i := 1; i <= 10; i++ {
        in <- i
    }
}()

for r := range b.Run(ctx, in) {
    if r.IsErr() {
        fmt.Println("error:", r.Err)
        continue
    }
    fmt.Println("batch:", r.Value) // [1 2 3], [4 5 6], [7 8 9], [10]
}
```

### Realtime batching

Uses a sliding window instead of a fixed ticker. The window resets on every new item. Only flushes when no items arrive for the full window duration. Useful for event streams where you want to wait for a burst to settle.

```go
gob := batch.NewRealtime[int](100, 100*time.Millisecond)

for r := range b.Run(ctx, in) {
    fmt.Println("batch:", r.Value)
}
```

### FlatMap

Expand one item into many. Useful for unpacking nested data, generating multiple downstream jobs from a single input, or splitting records.

```go
out := stream.FlatMap(ctx, in, 2,
    func(ctx context.Context, orderID int) ([]LineItem, error) {
        return fetchLineItems(orderID) // one order → many line items
    })

for r := range out {
    fmt.Println("line item:", r.Value)
}
```

### Parallel map

Transform items concurrently. Use `Map` when order doesn't matter, `OrderedMap` when it does.

```go
// unordered - faster
out := stream.Map(ctx, in, 5, func(ctx context.Context, url string) ([]byte, error) {
    return fetch(url)
})

// ordered - preserves input sequence
out := stream.OrderedMap(ctx, in, 5, func(ctx context.Context, url string) ([]byte, error) {
    return fetch(url)
})
```

### Backpressure

Parallel map with a concurrency cap. When all worker slots are full, reading from the input channel blocks naturally, slowing the producer down and preventing unbounded memory growth.
```go
goout := stream.BackpressureMap(ctx, in, 3,
    func(ctx context.Context, n int) (int, error) {
        return process(n)
    })

for r := range out {
    fmt.Println(r.Value)
}
```

### Graceful termination

Signal shutdown and wait for all in-flight work to drain before the program exits. No work is cut off mid-execution.

```go
goterm := termination.New()

for i := 0; i < 5; i++ {
    if !term.Track() {
        continue // already stopped, do not start new work
    }
    go func() {
        defer term.Done()
        doWork()
    }()
}

term.Stop() // signal shutdown - no new work accepted
term.Wait() // blocks until all in-flight goroutines call Done()
```

### Iterators

Consume any channel-based output using composable iterators - no manual channel loops.

```go
seq := iter.FromChan(ctx, numbersChan)

result := iter.Collect(
    iter.Map(
        iter.Filter(seq, func(n int) bool { return n%2 == 0 }),
        func(n int) int { return n * 2 },
    ),
)

fmt.Println(result) // all even numbers, doubled
```

---

## Result type

Every concurrent operation in FlowChan returns `chan Result[T]`. This carries either a value or an error - never just one or the other - so errors are never silently dropped.

```go
for r := range out {
    val, err := r.Unwrap()
    if err != nil {
        // handle error
        continue
    }
    // use val
}

// or use helpers
if r.IsErr() { ... }

// transform a result without unwrapping
mapped := errors.Map(r, func(n int) string {
    return fmt.Sprintf("id-%d", n)
})

// split a slice of results into values and errors
values, errs := errors.Collect(results)
```

---

## Context and cancellation

Every FlowChan operation respects context cancellation. Pass a context with a timeout or cancel function and all goroutines will exit cleanly.

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

wp := pool.NewWorkPool(tasks, 5)
errs := wp.Run(ctx) // stops cleanly when context times out
```

---

## Running tests

```bash
# test everything
go test ./... -race

# test a specific package
go test ./pool/... -v -race

# test with timeout (catches hangs)
go test ./... -race -timeout 30s
```

All packages are tested with the `-race` flag to catch data races.

---

## Project structure

```
flowchan/
├── go.mod
├── pool/               # Task interface, WorkPool, retries
├── errors/             # Result[T] - value + error carrier
├── pipeline/
│   ├── stage.go        # Stage[In,Out], Chain
│   └── ordered.go      # OrderedStage - preserves input order
├── batch/
│   ├── batch.go        # fixed size + timeout batching
│   └── realtime.go     # sliding window realtime batching
├── stream/
│   ├── stream.go       # Map, OrderedMap, FlatMap
│   └── backpressure.go # BackpressureMap - concurrency cap
├── termination/        # graceful shutdown, drain in-flight work
├── iter/               # Seq iterators, Filter, Map, Collect
└── example/            # end-to-end usage of all packages
```

### Dependency order

Packages only depend downward. Nothing in pool imports pipeline. Nothing imports iter.

```
iter
  |
pool  pipeline  batch  stream  termination
  |       |       |      |
         errors
```

This means any package can be used independently without pulling in the rest of the library.

---

## What's coming

Error propagation through chained pipeline stages
Goroutine reuse and pool resizing
Metrics and observability hooks
Rate limiting stage

---

## Inspired by

- [sourcegraph/conc](https://github.com/sourcegraph/conc) - structured concurrency for Go
- [panjf2000/ants](https://github.com/panjf2000/ants) - goroutine pool
- [reugn/go-streams](https://github.com/reugn/go-streams) - stream processing

---

## License

MIT