package pbft

import (
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"

	"wb/internal/common"
	"wb/internal/fault"
	"wb/internal/testnet"
	"wb/pb"
)

func cluster(t *testing.T, port int) (*Node, []*testnet.Replica) {
	p, _, rs := clusterNodes(t, port)
	return p, rs
}

func clusterNodes(t *testing.T, port int) (*Node, []*Node, []*testnet.Replica) {
	var nodes []*Node // replicas register in order r0, r1, ...
	rs := testnet.Start(t, 4, port, func(r *testnet.Replica, conns map[string]*grpc.ClientConn, gs *grpc.Server) {
		nd := New(r.ID, 4, conns, r.Auth, r.Inj, r.OnCommit)
		pb.RegisterPbftServer(gs, nd)
		nodes = append(nodes, nd)
	})
	for _, nd := range nodes {
		nd.Run()
		t.Cleanup(nd.Stop)
	}
	return nodes[0], nodes, rs
}

func op(i int) []byte {
	return []byte(fmt.Sprintf(`{"kind":"draw","id":"s%d","shape":{"color":"#0000ff"}}`, i))
}

func committedOn(rs []*testnet.Replica, idx uint64, skip string) int {
	c := 0
	for _, r := range rs {
		if _, ok := r.Committed()[idx]; ok && r.ID != skip {
			c++
		}
	}
	return c
}

// committedAnywhere counts replicas outside skip that committed exactly this op at some index.
func committedAnywhere(rs []*testnet.Replica, want []byte, skip string) int {
	c := 0
	for _, r := range rs {
		if r.ID == skip {
			continue
		}
		for _, o := range r.Committed() {
			if string(o) == string(want) {
				c++
				break
			}
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

// The signed pre-prepares that replicas forward in their prepares let honest replicas
// hold proof, checkable by anyone with the public keys, that the primary equivocated.
func TestEquivocatingPrimaryLeavesSignedEvidence(t *testing.T) {
	p, nodes, rs := clusterNodes(t, 17140)
	rs[0].Inj.Set(fault.Equivocate, 0)
	p.Propose(op(1))
	testnet.Eventually(t, 3*time.Second, "every honest replica holds evidence", func() bool {
		for _, nd := range nodes[1:] {
			if len(nd.Evidence()) == 0 {
				return false
			}
		}
		return true
	})
	outsider := rs[3].Auth // verifies with public keys only
	for _, nd := range nodes[1:] {
		for _, e := range nd.Evidence() {
			if e.Accused != "r0" || !e.Check(outsider) {
				t.Errorf("%s holds evidence that does not verify: accused %s", nd.id, e.Accused)
			}
		}
	}
}

// An honest primary must never be accused, even when a backup lies.
func TestNoEvidenceAgainstHonestPrimary(t *testing.T) {
	p, nodes, rs := clusterNodes(t, 17150)
	rs[1].Inj.Set(fault.Equivocate, 0)
	for i := 1; i <= 3; i++ {
		p.Propose(op(i))
	}
	testnet.Eventually(t, 3*time.Second, "honest replicas commit", func() bool { return committedOn(rs, 3, "r1") == 3 })
	for _, nd := range nodes {
		if ev := nd.Evidence(); len(ev) > 0 {
			t.Errorf("%s accused the honest primary: %+v", nd.id, ev)
		}
	}
}

// Scenario S1: an equivocating primary cannot make honest replicas disagree. The signed
// proof makes the backups move to view 1 at once, where r1 is primary and work resumes.
func TestEquivocatingPrimaryIsReplaced(t *testing.T) {
	p, nodes, rs := clusterNodes(t, 17130)
	p.Propose(op(1))
	testnet.Eventually(t, 3*time.Second, "op 1 commits", func() bool { return committedOn(rs, 1, "") == 4 })
	rs[0].Inj.Set(fault.Equivocate, 0)
	p.Propose(op(2))
	p.Propose(op(3))
	testnet.Eventually(t, 3*time.Second, "honest replicas move to view 1", func() bool {
		for _, nd := range nodes[1:] {
			if nd.Status()["view"].(uint64) != 1 || nd.Status()["changing"].(bool) {
				return false
			}
		}
		return true
	})
	if _, err := nodes[1].Propose(op(4)); err != nil {
		t.Fatal("r1 should be primary of view 1:", err)
	}
	testnet.Eventually(t, 3*time.Second, "the new primary's op commits on honest replicas", func() bool {
		return committedAnywhere(rs, op(4), "r0") == 3
	})
	testnet.Agree(t, rs, "r0")
}

// With client-signed requests, the lying primary's altered copy fails its client's
// signature: backups refuse it and replace the primary, so only what the client really
// sent can ever commit (validity), even though the lie had reached two backups.
func TestAlteredRequestIsRefused(t *testing.T) {
	p, nodes, rs := clusterNodes(t, 17180)
	for _, nd := range nodes {
		nd.Valid = common.ValidBatch
	}
	priv, id := common.NewClientKey()
	req := func(i int) []byte {
		return common.SignRequest(priv, []byte(fmt.Sprintf(`{"kind":"draw","id":"s%d","client":"%s"}`, i, id)))
	}
	rs[0].Inj.Set(fault.Equivocate, 0)
	p.Propose(req(1))
	testnet.Eventually(t, 3*time.Second, "honest replicas leave view 0", func() bool {
		for _, nd := range nodes[1:] {
			if nd.Status()["view"].(uint64) != 1 || nd.Status()["changing"].(bool) {
				return false
			}
		}
		return true
	})
	nodes[1].Propose(req(1)) // the client re-sends to the new primary
	testnet.Eventually(t, 3*time.Second, "the real request commits", func() bool {
		return committedAnywhere(rs, req(1), "r0") == 3
	})
	for _, r := range rs[1:] {
		for i, op := range r.Committed() {
			if string(op) != "[]" && !common.ValidBatch(op) {
				t.Errorf("%s committed an altered request at %d: %s", r.ID, i, op)
			}
		}
	}
	testnet.Agree(t, rs, "r0")
}

// A crashed primary is replaced once the backups suspect it (the server does this when a
// request it watched is not executed in time); prepared work below and above the stable
// checkpoint survives the change.
func TestViewChangeReplacesCrashedPrimary(t *testing.T) {
	p, nodes, rs := clusterNodes(t, 17160)
	for i := 1; i <= 40; i++ {
		p.Propose(op(i))
	}
	testnet.Eventually(t, 5*time.Second, "40 ops commit", func() bool { return committedOn(rs, 40, "") == 4 })
	testnet.Eventually(t, 3*time.Second, "checkpoint 32 is stable", func() bool {
		for _, nd := range nodes {
			if nd.Status()["stable"].(uint64) != 32 {
				return false
			}
		}
		return true
	})
	rs[0].Inj.Set(fault.Crash, 0)
	for _, nd := range nodes[1:] {
		nd.Suspect()
	}
	testnet.Eventually(t, 3*time.Second, "r1 leads view 1", func() bool {
		_, self := nodes[1].Leader()
		return self
	})
	for i := 41; i <= 45; i++ {
		if _, err := nodes[1].Propose(op(i)); err != nil {
			t.Fatal(err)
		}
	}
	testnet.Eventually(t, 3*time.Second, "new ops commit on the 3 live replicas", func() bool {
		return committedAnywhere(rs, op(45), "r0") == 3
	})
	testnet.Agree(t, rs)
}

// If the primary of the next view is down, the view change times out and moves on: here r1
// (primary of view 1) is crashed, so the replicas end up in view 2 under r2.
func TestViewChangeSkipsACrashedNextPrimary(t *testing.T) {
	_, nodes, rs := clusterNodes(t, 17170)
	rs[1].Inj.Set(fault.Crash, 0)
	for _, i := range []int{0, 2, 3} {
		nodes[i].Suspect()
	}
	testnet.Eventually(t, 4*time.Second, "r2 leads view 2", func() bool {
		_, self := nodes[2].Leader()
		return self && nodes[2].Status()["view"].(uint64) == 2
	})
	nodes[2].Propose(op(1))
	testnet.Eventually(t, 3*time.Second, "op commits on the 3 live replicas", func() bool {
		return committedAnywhere(rs, op(1), "r1") == 3
	})
	testnet.Agree(t, rs)
}

// A view change started while the primary is busy (a false suspicion) must still finish:
// slots in flight are carried over or nulled, and the new primary keeps committing.
func TestViewChangeUnderLoad(t *testing.T) {
	p, nodes, rs := clusterNodes(t, 17200)
	stop := make(chan struct{})
	go func() {
		for i := 1; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			p.Propose(op(i))
			time.Sleep(2 * time.Millisecond)
		}
	}()
	time.Sleep(300 * time.Millisecond)
	for _, nd := range nodes[1:] {
		go nd.Suspect()
	}
	time.Sleep(200 * time.Millisecond)
	close(stop)
	// Like a client: find whichever replica now leads (a slow machine may skip a view)
	// and send it the request, again if the view moves on before it commits.
	asked := map[*Node]bool{}
	testnet.Eventually(t, 20*time.Second, "a new primary commits on 2f+1 replicas", func() bool {
		for _, nd := range nodes {
			if _, self := nd.Leader(); self && nd.Status()["view"].(uint64) >= 1 && !asked[nd] {
				if _, err := nd.Propose(op(100000)); err == nil {
					asked[nd] = true
				}
			}
		}
		return committedAnywhere(rs, op(100000), "") >= 3
	})
	testnet.Agree(t, rs)
}

// State transfer: a backup cut off while the others commit (and garbage-collect up to a
// stable checkpoint) fetches what it missed once it hears 2f+1 checkpoints past it.
func TestLaggingReplicaCatchesUp(t *testing.T) {
	p, _, rs := clusterNodes(t, 17210)
	rs[3].Inj.Set(fault.Crash, 0)
	for i := 1; i <= 70; i++ {
		p.Propose(op(i))
	}
	testnet.Eventually(t, 15*time.Second, "70 ops commit without r3", func() bool { return committedOn(rs, 70, "r3") == 3 })
	rs[3].Inj.Set(fault.None, 0)
	for i := 71; i <= 100; i++ {
		p.Propose(op(i))
	}
	testnet.Eventually(t, 15*time.Second, "r3 catches up to 96 and beyond", func() bool { return committedOn(rs, 100, "") == 4 })
	testnet.Agree(t, rs)
}
