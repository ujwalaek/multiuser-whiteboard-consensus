package fault

import (
	"encoding/json"
	"math/rand"
	"sync"
)

type Mode string

const (
	None       Mode = "none"
	Crash      Mode = "crash"       // drop everything in and out
	Omit       Mode = "omit"        // drop a fraction p of messages, never lie
	Equivocate Mode = "equivocate"  // Byzantine: send inconsistent messages to different peers
)

type Injector struct {
	mu   sync.RWMutex
	mode Mode
	p    float64
}

func New() *Injector { return &Injector{mode: None} }

func (f *Injector) Set(m Mode, p float64) { f.mu.Lock(); f.mode, f.p = m, p; f.mu.Unlock() }
func (f *Injector) Mode() Mode            { f.mu.RLock(); defer f.mu.RUnlock(); return f.mode }
func (f *Injector) P() float64            { f.mu.RLock(); defer f.mu.RUnlock(); return f.p }
func (f *Injector) Equivocating() bool    { return f.Mode() == Equivocate }

// Drop reports whether the current message should be silently dropped.
func (f *Injector) Drop() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mode == Crash || (f.mode == Omit && rand.Float64() < f.p)
}

// Mutate returns a visibly different but still-valid version of a whiteboard op
// (shape turns red, "tampered" flag set) so divergence is visible on screen.
func Mutate(op []byte) []byte {
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