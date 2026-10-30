package raft

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"wb/internal/common"
	"wb/internal/fault"
	"wb/pb"
)

const (
	Follower = iota
	Candidate
	Leader
)

type Node struct {
	pb.UnimplementedRaftServer
	mu    sync.Mutex
	id    string
	n     int
	conns map[string]*grpc.ClientConn
	auth  *common.Auth
	inj   *fault.Injector
	onCommit common.CommitFn

	term      uint64
	votedFor  string
	log       []*pb.LogEntry // log[0] is a dummy entry
	commit    uint64
	applied   uint64
	state     int
	leader    string
	next      map[string]uint64
	match     map[string]uint64
	lastHeard time.Time
	timeout   time.Duration
	fast      bool // short election timeout so this node wins first election (demo determinism)
	done      chan struct{}
}

func New(id string, n int, conns map[string]*grpc.ClientConn, inj *fault.Injector,
	cb common.CommitFn, fast bool) *Node {
	nd := &Node{id: id, n: n, conns: conns, auth: common.NewAuth(id), inj: inj, onCommit: cb,
		log: []*pb.LogEntry{{}}, next: map[string]uint64{}, match: map[string]uint64{}, fast: fast,
		done: make(chan struct{})}
	nd.resetTimer()
	return nd
}

func (n *Node) resetTimer() { // caller holds mu (or is constructor)
	n.lastHeard = time.Now()
	n.timeout = time.Duration(500+rand.Intn(400)) * time.Millisecond
	if n.fast {
		n.timeout = 300 * time.Millisecond
	}
}

func (n *Node) stepDown(term uint64) { // caller holds mu
	n.term, n.state, n.votedFor = term, Follower, ""
	n.resetTimer()
}

func (n *Node) Run() { go n.loop(); go n.applier() }

// Stop ends the election/heartbeat and apply loops.
func (n *Node) Stop() { close(n.done) }

// every calls f on each tick of period d until the node is stopped.
func (n *Node) every(d time.Duration, f func()) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-n.done:
			return
		case <-t.C:
			f()
		}
	}
}

func (n *Node) loop() { n.every(50*time.Millisecond, n.tick) }

func (n *Node) tick() {
	if n.inj.Mode() == fault.Crash {
		return
	}
	n.mu.Lock()
	st, due := n.state, time.Since(n.lastHeard) > n.timeout
	n.mu.Unlock()
	if st == Leader {
		n.broadcast() // heartbeat + replication
	} else if due {
		n.startElection()
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.state, n.votedFor = Candidate, n.id
	n.term++
	n.resetTimer()
	term, li := n.term, uint64(len(n.log)-1)
	lt := n.log[li].Term
	n.mu.Unlock()

	votes := 1
	for p, c := range n.conns {
		go func(p string, c *grpc.ClientConn) {
			if n.inj.Drop() {
				return
			}
			req := &pb.VoteReq{Term: term, Candidate: n.id, LastIndex: li, LastTerm: lt, Sender: n.id}
			n.auth.Sign(p, req)
			ctx, cancel := common.Ctx()
			defer cancel()
			resp, err := pb.NewRaftClient(c).RequestVote(ctx, req)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if resp.Term > n.term {
				n.stepDown(resp.Term)
				return
			}
			if n.state == Candidate && n.term == term && resp.Granted {
				votes++
				if votes > n.n/2 {
					n.state, n.leader = Leader, n.id
					for q := range n.conns {
						n.next[q], n.match[q] = uint64(len(n.log)), 0
					}
				}
			}
		}(p, c)
	}
}

func (n *Node) RequestVote(_ context.Context, r *pb.VoteReq) (*pb.VoteResp, error) {
	if !n.auth.Verify(r.Sender, r) {
		return nil, status.Error(codes.Unauthenticated, "bad mac")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if r.Term > n.term {
		n.stepDown(r.Term)
	}
	li := uint64(len(n.log) - 1)
	lt := n.log[li].Term
	upToDate := r.LastTerm > lt || (r.LastTerm == lt && r.LastIndex >= li)
	ok := r.Term == n.term && (n.votedFor == "" || n.votedFor == r.Candidate) && upToDate
	if ok {
		n.votedFor = r.Candidate
		n.resetTimer()
	}
	return &pb.VoteResp{Term: n.term, Granted: ok}, nil
}

func (n *Node) broadcast() {
	for p := range n.conns {
		go n.replicate(p)
	}
}

func (n *Node) replicate(p string) {
	if n.inj.Drop() {
		return
	}
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	next := n.next[p]
	prev := next - 1
	entries := append([]*pb.LogEntry(nil), n.log[next:]...)
	term := n.term
	req := &pb.AppendReq{Term: term, Leader: n.id, PrevIndex: prev, PrevTerm: n.log[prev].Term,
		LeaderCommit: n.commit, Sender: n.id}
	n.mu.Unlock()

	// ---- Byzantine hook: tell odd-numbered peers a different story ----
	if n.inj.Equivocating() && common.Num(p)%2 == 1 {
		for i, e := range entries {
			entries[i] = &pb.LogEntry{Term: e.Term, Op: fault.Mutate(e.Op)}
		}
	}
	req.Entries = entries
	n.auth.Sign(p, req)

	ctx, cancel := common.Ctx()
	defer cancel()
	resp, err := pb.NewRaftClient(n.conns[p]).AppendEntries(ctx, req)
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.Term > n.term {
		n.stepDown(resp.Term)
		return
	}
	if n.state != Leader || n.term != term {
		return
	}
	if resp.Success {
		if m := prev + uint64(len(entries)); m > n.match[p] {
			n.match[p], n.next[p] = m, m+1
			n.advanceCommit()
		}
	} else if n.next[p] > 1 {
		n.next[p]--
	}
}

func (n *Node) advanceCommit() { // caller holds mu
	for idx := uint64(len(n.log) - 1); idx > n.commit; idx-- {
		if n.log[idx].Term != n.term {
			break
		}
		c := 1
		for _, m := range n.match {
			if m >= idx {
				c++
			}
		}
		if c > n.n/2 {
			n.commit = idx
			break
		}
	}
}

func (n *Node) AppendEntries(_ context.Context, r *pb.AppendReq) (*pb.AppendResp, error) {
	if !n.auth.Verify(r.Sender, r) {
		return nil, status.Error(codes.Unauthenticated, "bad mac")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if r.Term < n.term {
		return &pb.AppendResp{Term: n.term}, nil
	}
	if r.Term > n.term {
		n.term, n.votedFor = r.Term, ""
	}
	n.state, n.leader = Follower, r.Leader
	n.resetTimer()
	if r.PrevIndex >= uint64(len(n.log)) || n.log[r.PrevIndex].Term != r.PrevTerm {
		return &pb.AppendResp{Term: n.term}, nil
	}
	for i, e := range r.Entries {
		idx := r.PrevIndex + 1 + uint64(i)
		if idx < uint64(len(n.log)) {
			if n.log[idx].Term != e.Term { // conflict: truncate (term check only -- this is why Raft is blind to equivocation)
				n.log = append(n.log[:idx], e)
			}
		} else {
			n.log = append(n.log, e)
		}
	}
	last := r.PrevIndex + uint64(len(r.Entries))
	if r.LeaderCommit > n.commit {
		n.commit = min(r.LeaderCommit, last)
	}
	return &pb.AppendResp{Term: n.term, Success: true, MatchIndex: last}, nil
}

func (n *Node) applier() { n.every(20*time.Millisecond, n.apply) }

func (n *Node) apply() {
	n.mu.Lock()
	var ops [][]byte
	var idx []uint64
	for n.applied < n.commit {
		n.applied++
		ops = append(ops, n.log[n.applied].Op)
		idx = append(idx, n.applied)
	}
	n.mu.Unlock()
	for i := range ops {
		n.onCommit(idx[i], ops[i])
	}
}

func (n *Node) Propose(op []byte) (string, error) {
	if n.inj.Mode() == fault.Crash {
		return "", errors.New("crashed")
	}
	n.mu.Lock()
	if n.state != Leader {
		h := n.leader
		n.mu.Unlock()
		return h, common.ErrNotLeader
	}
	n.log = append(n.log, &pb.LogEntry{Term: n.term, Op: op})
	n.mu.Unlock()
	n.broadcast()
	return n.id, nil
}

func (n *Node) Status() map[string]any {
	n.mu.Lock()
	defer n.mu.Unlock()
	return map[string]any{"role": []string{"follower", "candidate", "leader"}[n.state],
		"term": n.term, "leader": n.leader, "commit": n.commit}
}