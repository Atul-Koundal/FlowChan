package stream

import (
	"context"
	"sync"
	"testing"

	fresult "github.com/Atul-Koundal/FlowChan/result"
)

func toResultChan[T any](items ...T) <-chan fresult.Result[T] {
	ch := make(chan fresult.Result[T], len(items))
	for _, item := range items {
		ch <- fresult.Result[T]{Value: item}
	}
	close(ch)
	return ch
}

func TestFanOut_DistributesItems(t *testing.T) {
	in := toResultChan(1, 2, 3, 4, 5, 6)
	outputs := FanOut(context.Background(), in, 3)

	var mu sync.Mutex
	var all []int
	var wg sync.WaitGroup

	for _, out := range outputs {
		wg.Add(1)
		go func(ch <-chan fresult.Result[int]) {
			defer wg.Done()
			for r := range ch {
				mu.Lock()
				all = append(all, r.Value)
				mu.Unlock()
			}
		}(out)
	}

	wg.Wait()

	if len(all) != 6 {
		t.Errorf("expected 6 items total, got %d", len(all))
	}
}

func TestFanOut_Cancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	in := toResultChan(1, 2, 3)
	outputs := FanOut(ctx, in, 2)

	for _, out := range outputs {
		for range out {
		}
	}
}

func TestFanIn_MergesChannels(t *testing.T) {
	ch1 := toResultChan(1, 2, 3)
	ch2 := toResultChan(4, 5, 6)

	out := FanIn(context.Background(), ch1, ch2)

	var results []int
	for r := range out {
		results = append(results, r.Value)
	}

	if len(results) != 6 {
		t.Errorf("expected 6 items, got %d", len(results))
	}
}

func TestFanIn_Cancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ch1 := toResultChan(1, 2, 3)
	ch2 := toResultChan(4, 5, 6)

	out := FanIn(ctx, ch1, ch2)
	for range out {
	}
}