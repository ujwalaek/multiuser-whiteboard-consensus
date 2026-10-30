package server

import "testing"

func TestLedgerChainsAndDeduplicates(t *testing.T) {
	s := &Server{}
	s.Init()
	s.OnCommit(1, []byte(`{"client":"c1","id":"s1"}`))
	s.OnCommit(2, []byte(`{"client":"c1","id":"s2"}`))
	s.OnCommit(3, []byte(`{"client":"c1","id":"s1"}`)) // retried request

	if i := VerifyChain(s.log); i != 0 {
		t.Fatalf("intact chain reported broken at %d", i)
	}
	if s.log[0].Prev != genesis || s.log[1].Prev != s.log[0].Hash {
		t.Fatal("blocks are not linked by prev hash")
	}
	if s.log[0].Dup || s.log[1].Dup || !s.log[2].Dup {
		t.Fatalf("dup flags = %v %v %v, want false false true", s.log[0].Dup, s.log[1].Dup, s.log[2].Dup)
	}

	s.log[1].Op = []byte(`{"client":"c1","id":"evil"}`) // tamper with history
	if i := VerifyChain(s.log); i != 2 {
		t.Fatalf("tampering at index 2 reported at %d", i)
	}
}

// Same ops in a different order give a different head: replicas that disagree on order fork.
func TestLedgerForksOnDifferentOrder(t *testing.T) {
	a, b := &Server{}, &Server{}
	a.Init()
	b.Init()
	a.OnCommit(1, []byte(`{"id":"x"}`))
	a.OnCommit(2, []byte(`{"id":"y"}`))
	b.OnCommit(1, []byte(`{"id":"y"}`))
	b.OnCommit(2, []byte(`{"id":"x"}`))
	if a.head() == b.head() {
		t.Fatal("different histories produced the same ledger head")
	}
}
