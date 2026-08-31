package iter

import (
	"context"
	"testing"

	fresult "github.com/Atul-Koundal/FlowChan/result"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestFromChan(t *testing.T) {
	ch := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		ch <- i
	}
	close(ch)

	seq := FromChan(context.Background(), ch)
	results := Collect(seq)

	if len(results) != 5 {
		t.Fatalf("expected 5 items, got %d", len(results))
	}
}

func TestFromResults(t *testing.T) {
	ch := make(chan fresult.Result[int], 3)
	ch <- fresult.Result[int]{Value: 1}
	ch <- fresult.Result[int]{Value: 2}
	ch <- fresult.Result[int]{Value: 3}
	close(ch)

	seq := FromResults(context.Background(), ch)

	var values []int
	seq(func(v int, err error) bool {
		if err == nil {
			values = append(values, v)
		}
		return true
	})

	if len(values) != 3 {
		t.Fatalf("expected 3 values, got %d", len(values))
	}
}

func TestMap(t *testing.T) {
	ch := make(chan int, 3)
	ch <- 1
	ch <- 2
	ch <- 3
	close(ch)

	seq := FromChan(context.Background(), ch)
	doubled := Map(seq, func(n int) int { return n * 2 })
	results := Collect(doubled)

	expected := []int{2, 4, 6}
	for i, v := range results {
		if v != expected[i] {
			t.Errorf("position %d: expected %d got %d", i, expected[i], v)
		}
	}
}

func TestFilter(t *testing.T) {
	ch := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		ch <- i
	}
	close(ch)

	seq := FromChan(context.Background(), ch)
	evens := Filter(seq, func(n int) bool { return n%2 == 0 })
	results := Collect(evens)

	if len(results) != 2 {
		t.Fatalf("expected 2 even numbers, got %d", len(results))
	}
}

func TestToChan(t *testing.T) {
	ch := make(chan int, 3)
	ch <- 10
	ch <- 20
	ch <- 30
	close(ch)

	seq := FromChan(context.Background(), ch)
	out := ToChan(context.Background(), seq)

	var results []int
	for v := range out {
		results = append(results, v)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 items, got %d", len(results))
	}
}

func TestCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ch := make(chan int, 5)
	for i := 0; i < 5; i++ {
		ch <- i
	}
	close(ch)

	seq := FromChan(ctx, ch)
	// should not hang
	Collect(seq)
}