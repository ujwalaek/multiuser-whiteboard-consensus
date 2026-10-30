package server

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"

	"wb/internal/fault"
)

func newServer() *Server {
	s := &Server{Inj: fault.New(), BatchMax: 1}
	s.Init()
	return s
}

func TestLedgerChainsAndDeduplicates(t *testing.T) {
	s := newServer()
	s.OnCommit(1, []byte(`[{"client":"c1","id":"s1"},{"client":"c1","id":"s2"}]`))
	s.OnCommit(2, []byte(`[{"client":"c1","id":"s1"}]`)) // retried request
	s.OnCommit(3, []byte(`{"client":"c2","id":"s1"}`))   // a bare op is a batch of one

	if i := VerifyChain(s.log); i != 0 {
		t.Fatalf("intact chain reported broken at %d", i)
	}
	if s.log[0].Prev != genesis || s.log[1].Prev != s.log[0].Hash {
		t.Fatal("blocks are not linked by prev hash")
	}
	if got := fmt.Sprint(s.log[0].Dup, s.log[1].Dup, s.log[2].Dup); got != "[false false] [true] [false]" {
		t.Fatalf("dup flags = %s", got)
	}

	s.log[0].Ops[1] = json.RawMessage(`{"client":"c1","id":"evil"}`) // tamper with history
	if i := VerifyChain(s.log); i != 1 {
		t.Fatalf("tampering in block 1 reported at %d", i)
	}
}

// Same ops in a different order give a different head: replicas that disagree on order fork.
func TestLedgerForksOnDifferentOrder(t *testing.T) {
	a, b := newServer(), newServer()
	a.OnCommit(1, []byte(`[{"id":"x"}]`))
	a.OnCommit(2, []byte(`[{"id":"y"}]`))
	b.OnCommit(1, []byte(`[{"id":"y"}]`))
	b.OnCommit(2, []byte(`[{"id":"x"}]`))
	if a.head() == b.head() {
		t.Fatal("different histories produced the same ledger head")
	}
}

func TestMerkleProofs(t *testing.T) {
	for n := 1; n <= 9; n++ {
		ops := make([][]byte, n)
		for i := range ops {
			ops[i] = []byte(fmt.Sprintf(`{"id":"op%d"}`, i))
		}
		root := MerkleRoot(ops)
		for i := range ops {
			p := MerkleProof(ops, i)
			if !VerifyProof(ops[i], p, root) {
				t.Fatalf("n=%d: proof for op %d does not verify", n, i)
			}
			if VerifyProof([]byte(`{"id":"forged"}`), p, root) {
				t.Fatalf("n=%d: forged op verified with op %d's proof", n, i)
			}
		}
	}
	// promoting an odd node, not duplicating it, keeps [a b c] and [a b c c] apart
	abc := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	if hex.EncodeToString(MerkleRoot(abc)) == hex.EncodeToString(MerkleRoot(append(abc, []byte("c")))) {
		t.Fatal("[a b c] and [a b c c] share a Merkle root")
	}
}

// A committed entry that is not valid JSON still takes its place in the chain, so
// replicas that committed different garbage still show up as a fork.
func TestUndecodableEntryIsKept(t *testing.T) {
	a, b := newServer(), newServer()
	a.OnCommit(1, []byte(`X[{"id":"1"}]`))
	b.OnCommit(1, []byte(`[{"id":"1"}]`))
	if len(a.log) != 1 || a.head() == b.head() {
		t.Fatal("an undecodable entry was dropped instead of chained")
	}
}

// A Byzantine replica's lie to its clients is a self-consistent block that differs
// from the honest one, so only comparing replicas exposes it.
func TestLieIsConsistentButDifferent(t *testing.T) {
	s := newServer()
	s.OnCommit(1, []byte(`[{"id":"x","shape":{"color":"#0000ff"}}]`))
	l := lie(s.log[0])
	if VerifyChain([]Block{l}) != 0 {
		t.Fatal("the lie does not verify on its own")
	}
	if l.Hash == s.log[0].Hash {
		t.Fatal("the lie has the honest hash")
	}
}
