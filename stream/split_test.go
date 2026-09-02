package stream

import (
	"context"
	"testing"
	"sync"
)

func TestSplit_DividesByPredicate(t *testing.T) {
	in := toResultChan(1, 2, 3, 4, 5, 6)

	evens, odds := Split(context.Background(), in, func(n int) bool {
		return n%2 == 0
	})

	var evenCount, oddCount int
	var wg sync.Mutex

	go func() {
		for range evens {
			wg.Lock()
			evenCount++
			wg.Unlock()
		}
	}()
	for range odds {
		wg.Lock()
		oddCount++
		wg.Unlock()
	}

	if evenCount != 3 {
		t.Errorf("expected 3 evens, got %d", evenCount)
	}
	if oddCount != 3 {
		t.Errorf("expected 3 odds, got %d", oddCount)
	}
}

func TestMerge_CombinesStreams(t *testing.T) {
	ch1 := toResultChan(1, 2, 3)
	ch2 := toResultChan(4, 5, 6)

	out := Merge(context.Background(), ch1, ch2)

	var count int
	for range out {
		count++
	}

	if count != 6 {
		t.Errorf("expected 6 items, got %d", count)
	}
}

func TestMerge_Cancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ch1 := toResultChan(1, 2, 3)
	out := Merge(ctx, ch1)
	for range out {
	}
}