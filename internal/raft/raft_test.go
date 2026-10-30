package raft

import (
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"

	"wb/internal/fault"
	"wb/internal/testnet"
	"wb/pb"
)

func cluster(t *testing.T, n, port int) ([]*Node, []*testnet.Replica) {
	var nodes []*Node // replicas register in order r0, r1, ...
	rs := testnet.Start(t, n, port, func(r *testnet.Replica, conns map[string]*grpc.ClientConn, gs *grpc.Server) {
		nd := New(r.ID, n, conns, r.Auth, r.Inj, r.OnCommit, false)
		pb.RegisterRaftServer(gs, nd)
		nodes = append(nodes, nd)
	})
	for _, nd := range nodes {
		nd.Run()
		t.Cleanup(nd.Stop)
	}
	return nodes, rs
}

func leaders(ns []*Node, rs []*testnet.Replica) []int {
	var ls []int
	for i, nd := range ns {
		if rs[i].Inj.Mode() != fault.Crash && nd.Status()["role"] == "leader" {
			ls = append(ls, i)
		}
	}
	return ls
}

func waitLeader(t *testing.T, ns []*Node, rs []*testnet.Replica) int {
	t.Helper()
	var l []int
	testnet.Eventually(t, 5*time.Second, "a single leader", func() bool { l = leaders(ns, rs); return len(l) == 1 })
	return l[0]
}

func op(i int) []byte { return []byte(fmt.Sprintf(`{"kind":"draw","id":"s%d","shape":{"color":"#0000ff"}}`, i)) }

func committedOn(rs []*testnet.Replica, idx uint64) int {
	c := 0
	for _, r := range rs {
		if _, ok := r.Committed()[idx]; ok {
			c++
		}
	}
	return c
}

func TestElectsOneLeader(t *testing.T) {
	ns, rs := cluster(t, 5, 17000)
	waitLeader(t, ns, rs)
}

func TestReplicatesToAll(t *testing.T) {
	ns, rs := cluster(t, 5, 17010)
	l := waitLeader(t, ns, rs)
	for i := 1; i <= 3; i++ {
		if _, err := ns[l].Propose(op(i)); err != nil {
			t.Fatal(err)
		}
	}
	testnet.Eventually(t, 3*time.Second, "all 5 replicas commit 3 ops", func() bool { return committedOn(rs, 3) == 5 })
	testnet.Agree(t, rs)
}

// N = 5 tolerates f = 2 crashes (N >= 2f+1); a third crash removes the majority and commits stall.
func TestCrashBoundary(t *testing.T) {
	ns, rs := cluster(t, 5, 17020)
	l := waitLeader(t, ns, rs)
	var down []int
	for i := range rs {
		if i != l && len(down) < 3 {
			down = append(down, i)
		}
	}
	rs[down[0]].Inj.Set(fault.Crash, 0)
	rs[down[1]].Inj.Set(fault.Crash, 0)
	ns[l].Propose(op(1))
	testnet.Eventually(t, 3*time.Second, "commit with 2 of 5 crashed", func() bool { return committedOn(rs, 1) == 3 })

	rs[down[2]].Inj.Set(fault.Crash, 0)
	ns[l].Propose(op(2))
	time.Sleep(1500 * time.Millisecond)
	if c := committedOn(rs, 2); c != 0 {
		t.Fatalf("op committed on %d replicas with only 2 of 5 alive", c)
	}

	rs[down[2]].Inj.Set(fault.None, 0)
	// any leader (old or newly elected) commits op 2 once a majority is back
	testnet.Eventually(t, 5*time.Second, "commit after healing one replica", func() bool {
		for _, i := range leaders(ns, rs) {
			ns[i].Propose(op(3))
		}
		return committedOn(rs, 2) >= 3
	})
	testnet.Agree(t, rs)
}

// Scenario S1: an equivocating leader makes honest followers commit different ops at the same index.
func TestEquivocatingLeaderBreaksSafety(t *testing.T) {
	ns, rs := cluster(t, 4, 17030)
	l := waitLeader(t, ns, rs)
	rs[l].Inj.Set(fault.Equivocate, 0)
	ns[l].Propose(op(1))
	testnet.Eventually(t, 3*time.Second, "index 1 committed on all replicas", func() bool { return committedOn(rs, 1) == 4 })

	seen := map[string]bool{}
	for i, r := range rs {
		if i != l {
			seen[string(r.Committed()[1])] = true
		}
	}
	if len(seen) < 2 {
		t.Fatalf("expected honest replicas to diverge under an equivocating leader, all committed %v", seen)
	}
}
