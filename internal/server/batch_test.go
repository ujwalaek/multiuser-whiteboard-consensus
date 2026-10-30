package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// Ops submitted together land in one entry, in a JSON array, and every submitter
// hears the proposal's result.
func TestBatcherGroupsConcurrentSubmits(t *testing.T) {
	var mu sync.Mutex
	var entries [][]json.RawMessage
	b := newBatcher(8, 20*time.Millisecond, func(e []byte) error {
		var ops []json.RawMessage
		json.Unmarshal(e, &ops)
		mu.Lock()
		entries = append(entries, ops)
		mu.Unlock()
		return nil
	})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := b.submit([]byte(fmt.Sprintf(`{"id":"%d"}`, i))); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	total := 0
	for _, e := range entries {
		total += len(e)
	}
	if total != 8 || len(entries) > 2 {
		t.Fatalf("8 concurrent ops became %d entries holding %d ops", len(entries), total)
	}
}

func TestBatcherReportsProposeError(t *testing.T) {
	b := newBatcher(1, time.Millisecond, func([]byte) error { return errors.New("not leader") })
	if b.submit([]byte(`{}`)) == nil {
		t.Fatal("proposal error was not passed back to the submitter")
	}
}
