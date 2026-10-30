package server

import (
	"encoding/json"
	"time"
)

type pending struct {
	op   json.RawMessage
	done chan error
}

// batcher groups ops submitted to the leader into one consensus entry: it proposes
// as soon as max ops are waiting or wait has passed since the first one arrived.
// Batching trades a little latency for far fewer consensus rounds per op.
type batcher struct {
	in      chan pending
	max     int
	wait    time.Duration
	propose func([]byte) error
}

func newBatcher(max int, wait time.Duration, propose func([]byte) error) *batcher {
	if max < 1 {
		max = 1
	}
	b := &batcher{in: make(chan pending, 4096), max: max, wait: wait, propose: propose}
	go b.run()
	return b
}

func (b *batcher) submit(op []byte) error {
	p := pending{op, make(chan error, 1)}
	b.in <- p
	return <-p.done
}

func (b *batcher) run() {
	for first := range b.in {
		batch := []pending{first}
		timer := time.NewTimer(b.wait)
	fill:
		for len(batch) < b.max {
			select {
			case p := <-b.in:
				batch = append(batch, p)
			case <-timer.C:
				break fill
			}
		}
		timer.Stop()
		ops := make([]json.RawMessage, len(batch))
		for i, p := range batch {
			ops[i] = p.op
		}
		entry, _ := json.Marshal(ops)
		err := b.propose(entry)
		for _, p := range batch {
			p.done <- err
		}
	}
}
