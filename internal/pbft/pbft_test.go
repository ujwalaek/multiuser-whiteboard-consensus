package pbft

import (
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"

	"wb/internal/fault"
	"wb/internal/testnet"
	"wb/pb"
)

func cluster(t *testing.T, port int) (*Node, []*testnet.Replica) {
	var primary *Node
	rs := testnet.Start(t, 4, port, func(r *testnet.Replica, conns map[string]*grpc.ClientConn, gs *grpc.Server) {
		nd := New(r.ID, 4, conns, r.Inj, r.OnCommit)
		pb.RegisterPbftServer(gs, nd)
		if r.ID == "r0" {
			primary = nd
		}
	})
	return primary, rs
}

func op(i int) []byte { return []byte(fmt.Sprintf(`{"kind":"draw","id":"s%d","shape":{"color":"#0000ff"}}`, i)) }

func committedOn(rs []*testnet.Replica, idx uint64, skip string) int {
	c := 0
	for _, r := range rs {
		if _, ok := r.Committed()[idx]; ok && r.ID != skip {
			c++
		}
	}
	return c
}

func TestCommitsInOrderOnAll(t *testing.T) {
	p, rs := cluster(t, 17100)
	for i := 1; i <= 5; i++ {
		if _, err := p.Propose(op(i)); err != nil {
			t.Fatal(err)
		}
	}
	testnet.Eventually(t, 3*time.Second, "all 4 replicas commit 5 ops", func() bool { return committedOn(rs, 5, "") == 4 })
	testnet.Agree(t, rs)
}

// N = 4 tolerates f = 1 faulty replica; a crashed backup does not stop progress.
func TestToleratesOneCrashedBackup(t *testing.T) {
	p, rs := cluster(t, 17110)
	rs[3].Inj.Set(fault.Crash, 0)
	p.Propose(op(1))
	testnet.Eventually(t, 3*time.Second, "3 live replicas commit", func() bool { return committedOn(rs, 1, "r3") == 3 })
	testnet.Agree(t, rs)
}

// Scenario S2: a backup that lies in its prepare/commit messages is outvoted; safety and liveness hold.
func TestEquivocatingBackupIsOutvoted(t *testing.T) {
	p, rs := cluster(t, 17120)
	rs[1].Inj.Set(fault.Equivocate, 0)
	p.Propose(op(1))
	testnet.Eventually(t, 3*time.Second, "honest replicas commit", func() bool { return committedOn(rs, 1, "r1") == 3 })
	testnet.Agree(t, rs, "r1")
}

// Scenario S1: an equivocating primary cannot make honest replicas disagree; without view change it only stalls.
func TestEquivocatingPrimaryStallsButStaysSafe(t *testing.T) {
	p, rs := cluster(t, 17130)
	rs[0].Inj.Set(fault.Equivocate, 0)
	p.Propose(op(1))
	p.Propose(op(2))
	time.Sleep(1500 * time.Millisecond)
	if c := committedOn(rs, 1, "r0"); c != 0 {
		t.Errorf("equivocated op committed on %d honest replicas; expected a stall", c)
	}
	testnet.Agree(t, rs, "r0")
}
