package stream

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	fresult "github.com/Atul-Koundal/FlowChan/result"
)

func feed(n int) <-chan int {
	ch := make(chan int, n)
	for i := 0; i < n; i++ {
		ch <- i
	}
	close(ch)
	return ch
}

func isPanic(err error) bool {
	var pe *fresult.PanicError
	return errors.As(err, &pe)
}

func TestMap_PanicBecomesErrorInsteadOfCrashing(t *testing.T) {
	out := Map(context.Background(), feed(5), 2, func(ctx context.Context, n int) (int, error) {
		if n == 2 {
			panic("bad input")
		}
		return n, nil
	})
	var panics, ok int
	for r := range out {
		if isPanic(r.Err) {
			panics++
		} else if r.Err == nil {
			ok++
		}
	}
	if panics != 1 || ok != 4 {
		t.Fatalf("panics=%d ok=%d", panics, ok)
	}
}

func TestOrderedMap_PanicAndOrderPreserved(t *testing.T) {
	out := OrderedMap(context.Background(), feed(20), 4, func(ctx context.Context, n int) (int, error) {
		if n == 7 {
			panic("bad")
		}
		time.Sleep(time.Duration(20-n) * time.Millisecond / 4)
		return n, nil
	})
	i := 0
	for r := range out {
		if i == 7 {
			if !isPanic(r.Err) {
				t.Fatalf("item 7 should be a panic error, got %v", r.Err)
			}
		} else if r.Err != nil || r.Value != i {
			t.Fatalf("position %d: got %+v", i, r)
		}
		i++
	}
	if i != 20 {
		t.Fatalf("got %d results, want 20", i)
	}
}

func TestOrderedMap_SlowHeadDoesNotLetWorkersRunUnbounded(t *testing.T) {
	var started atomic.Int32
	release := make(chan struct{})
	out := OrderedMap(context.Background(), feed(1000), 2, func(ctx context.Context, n int) (int, error) {
		started.Add(1)
		if n == 0 {
			<-release
		}
		return n, nil
	})
	time.Sleep(150 * time.Millisecond)
	// window = workers*4 = 8 items may be in flight/buffered while item 0 is stuck
	if got := started.Load(); got > 20 {
		t.Fatalf("%d items started while the head was blocked; reorder buffer is unbounded", got)
	}
	close(release)
	n := 0
	for range out {
		n++
	}
	if n != 1000 {
		t.Fatalf("got %d results", n)
	}
}

func TestFlatMap_PanicBecomesError(t *testing.T) {
	out := FlatMap(context.Background(), feed(3), 1, func(ctx context.Context, n int) ([]int, error) {
		if n == 1 {
			panic("x")
		}
		return []int{n, n}, nil
	})
	var panics, vals int
	for r := range out {
		if isPanic(r.Err) {
			panics++
		} else {
			vals++
		}
	}
	if panics != 1 || vals != 4 {
		t.Fatalf("panics=%d vals=%d", panics, vals)
	}
}

func TestBackpressureMap_PanicBecomesError(t *testing.T) {
	out := BackpressureMap(context.Background(), feed(3), 2, func(ctx context.Context, n int) (int, error) {
		panic("x")
	})
	for r := range out {
		if !isPanic(r.Err) {
			t.Fatalf("want panic error, got %v", r.Err)
		}
	}
}

// goleak (TestMain) fails the package if any of these leave goroutines behind.
func TestCancel_WithAbandonedConsumerLeavesNoGoroutines(t *testing.T) {
	slow := func(ctx context.Context, n int) (int, error) { return n, nil }
	ctx, cancel := context.WithCancel(context.Background())
	infinite := make(chan int)
	go func() {
		defer close(infinite)
		for i := 0; ; i++ {
			select {
			case infinite <- i:
			case <-ctx.Done():
				return
			}
		}
	}()
	_ = Map(ctx, (<-chan int)(infinite), 4, slow)
	_ = OrderedMap(ctx, (<-chan int)(infinite), 4, slow)
	_ = FlatMap(ctx, (<-chan int)(infinite), 4, func(ctx context.Context, n int) ([]int, error) { return []int{n, n, n}, nil })
	time.Sleep(50 * time.Millisecond) // consumers never read; outputs fill up
	cancel()
}

func TestFanOut_StalledConsumerDoesNotBlockOthers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := make(chan fresult.Result[int])
	outs := FanOut(ctx, in, 2)
	go func() {
		for i := 0; i < 50; i++ {
			select {
			case in <- fresult.Result[int]{Value: i}:
			case <-ctx.Done():
				return
			}
		}
		close(in)
	}()
	// outs[0] is never read. outs[1] must still receive nearly everything.
	var got int
	timeout := time.After(2 * time.Second)
loop:
	for {
		select {
		case _, ok := <-outs[1]:
			if !ok {
				break loop
			}
			got++
		case <-timeout:
			break loop
		}
	}
	if got < 45 {
		t.Fatalf("healthy consumer only received %d/50 items", got)
	}
}

func TestFanOut_NonPositiveNDoesNotCrash(t *testing.T) {
	in := make(chan fresult.Result[int], 1)
	in <- fresult.Result[int]{Value: 1}
	close(in)
	outs := FanOut(context.Background(), in, 0)
	if len(outs) != 1 {
		t.Fatalf("want 1 output, got %d", len(outs))
	}
	if r := <-outs[0]; r.Value != 1 {
		t.Fatalf("got %+v", r)
	}
}

func TestSplit_PredicatePanicBecomesErrorOnMatched(t *testing.T) {
	in := make(chan fresult.Result[int], 1)
	in <- fresult.Result[int]{Value: 1}
	close(in)
	m, u := Split(context.Background(), in, func(int) bool { panic("p") })
	go func() {
		for range u {
		}
	}()
	r := <-m
	if !isPanic(r.Err) {
		t.Fatalf("got %+v", r)
	}
}

func TestMap_ZeroWorkersStillProcesses(t *testing.T) {
	n := 0
	for range Map(context.Background(), feed(3), 0, func(ctx context.Context, i int) (int, error) { return i, nil }) {
		n++
	}
	if n != 3 {
		t.Fatalf("got %d", n)
	}
}
