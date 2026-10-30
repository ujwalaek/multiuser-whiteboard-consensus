package fault

import (
	"encoding/json"
	"math/rand"
	"sync"
	"time"
)

type Mode string

const (
	None       Mode = "none"
	Crash      Mode = "crash"      // drop everything in and out
	Omit       Mode = "omit"       // drop a fraction p of messages, never lie
	Partition  Mode = "partition"  // exchange messages only with the listed peers
	Equivocate Mode = "equivocate" // Byzantine: send inconsistent messages to different peers
)

type Injector struct {
	mu    sync.RWMutex
	mode  Mode
	p     float64
	peers map[string]bool // reachable peers under Partition
	delay time.Duration   // added to every outgoing message, independent of mode
}

func New() *Injector { return &Injector{mode: None} }

func (f *Injector) Set(m Mode, p float64) { f.mu.Lock(); f.mode, f.p = m, p; f.mu.Unlock() }

// SetPartition limits this replica to exchanging messages with peers.
func (f *Injector) SetPartition(peers []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mode, f.peers = Partition, map[string]bool{}
	for _, p := range peers {
		f.peers[p] = true
	}
}

func (f *Injector) SetDelay(d time.Duration) { f.mu.Lock(); f.delay = d; f.mu.Unlock() }
func (f *Injector) Delay() time.Duration     { f.mu.RLock(); defer f.mu.RUnlock(); return f.delay }
func (f *Injector) Mode() Mode               { f.mu.RLock(); defer f.mu.RUnlock(); return f.mode }
func (f *Injector) P() float64               { f.mu.RLock(); defer f.mu.RUnlock(); return f.p }
func (f *Injector) Equivocating() bool       { return f.Mode() == Equivocate }

func (f *Injector) Peers() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var ps []string
	for p := range f.peers {
		ps = append(ps, p)
	}
	return ps
}

// Drop reports whether a message to or from peer should be silently dropped.
func (f *Injector) Drop(peer string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	switch f.mode {
	case Crash:
		return true
	case Omit:
		return rand.Float64() < f.p
	case Partition:
		return !f.peers[peer]
	}
	return false
}

// Mutate returns a visibly different but still-valid version of a whiteboard op
// (shape turns red, "tampered" flag set) so divergence is visible on screen.
// A batch (JSON array of ops) has every op mutated.
func Mutate(op []byte) []byte {
	var batch []json.RawMessage
	if json.Unmarshal(op, &batch) == nil {
		for i, o := range batch {
			batch[i] = Mutate(o)
		}
		b, _ := json.Marshal(batch)
		return b
	}
	var m map[string]any
	if json.Unmarshal(op, &m) != nil {
		return append([]byte("X"), op...)
	}
	m["tampered"] = true
	if s, ok := m["shape"].(map[string]any); ok {
		s["color"] = "#ff0000"
	}
	b, _ := json.Marshal(m)
	return b
}
