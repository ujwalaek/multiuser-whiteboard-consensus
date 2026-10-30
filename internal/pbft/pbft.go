package pbft

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"wb/internal/common"
	"wb/internal/fault"
	"wb/pb"
)

type slot struct {
	op, digest []byte
	pre, sentC, done bool
	prep, comm map[string]string // sender -> hex digest they claim
}

type Node struct {
	pb.UnimplementedPbftServer
	mu       sync.Mutex
	applyMu  sync.Mutex // guarantees onCommit is called in sequence order
	id       string
	primary  string
	n, f     int
	view     uint64
	seq      uint64
	exec     uint64 // next seq to execute
	slots    map[uint64]*slot
	conns    map[string]*grpc.ClientConn
	auth     *common.Auth
	inj      *fault.Injector
	onCommit common.CommitFn
}

func New(id string, n int, conns map[string]*grpc.ClientConn, inj *fault.Injector, cb common.CommitFn) *Node {
	return &Node{id: id, primary: "r0", n: n, f: (n - 1) / 3, exec: 1, slots: map[uint64]*slot{},
		conns: conns, auth: common.NewAuth(id), inj: inj, onCommit: cb}
}

func (n *Node) slot(s uint64) *slot { // caller holds mu
	x := n.slots[s]
	if x == nil {
		x = &slot{prep: map[string]string{}, comm: map[string]string{}}
		n.slots[s] = x
	}
	return x
}

func (n *Node) byz(p string) bool { return n.inj.Equivocating() && common.Num(p)%2 == 1 }

func (n *Node) digestFor(p string, d []byte) []byte {
	if n.byz(p) { // Byzantine: lie about what I received
		x := sha256.Sum256(d)
		return x[:]
	}
	return d
}

func (n *Node) Propose(op []byte) (string, error) {
	if n.id != n.primary {
		return n.primary, common.ErrNotLeader
	}
	if n.inj.Mode() == fault.Crash {
		return "", errors.New("crashed")
	}
	d := sha256.Sum256(op)
	n.mu.Lock()
	n.seq++
	seq := n.seq
	s := n.slot(seq)
	s.op, s.digest, s.pre = op, d[:], true
	n.mu.Unlock()
	for p, c := range n.conns {
		go func(p string, c *grpc.ClientConn) {
			if n.inj.Drop() {
				return
			}
			o := op
			if n.byz(p) {
				o = fault.Mutate(op)
			}
			dd := sha256.Sum256(o)
			m := &pb.PrePrepareMsg{View: n.view, Seq: seq, Digest: dd[:], Op: o, Sender: n.id}
			n.auth.Sign(p, m)
			ctx, cancel := common.Ctx()
			defer cancel()
			pb.NewPbftClient(c).PrePrepare(ctx, m)
		}(p, c)
	}
	return n.id, nil
}

func (n *Node) PrePrepare(_ context.Context, m *pb.PrePrepareMsg) (*pb.Ack, error) {
	if !n.auth.Verify(m.Sender, m) || m.Sender != n.primary || m.View != n.view {
		return nil, status.Error(codes.PermissionDenied, "rejected")
	}
	if d := sha256.Sum256(m.Op); !bytes.Equal(d[:], m.Digest) {
		return nil, status.Error(codes.InvalidArgument, "digest mismatch")
	}
	n.mu.Lock()
	s := n.slot(m.Seq)
	if s.pre && !bytes.Equal(s.digest, m.Digest) {
		n.mu.Unlock()
		return nil, status.Error(codes.AlreadyExists, "conflicting pre-prepare") // primary equivocation detected
	}
	s.op, s.digest, s.pre = m.Op, m.Digest, true
	s.prep[n.id] = hex.EncodeToString(m.Digest)
	n.mu.Unlock()
	n.bcast(true, m.Seq, m.Digest)
	n.advance(m.Seq)
	return &pb.Ack{}, nil
}

func (n *Node) Prepare(_ context.Context, m *pb.PhaseMsg) (*pb.Ack, error) { return n.phase(m, true) }
func (n *Node) Commit(_ context.Context, m *pb.PhaseMsg) (*pb.Ack, error)  { return n.phase(m, false) }

func (n *Node) phase(m *pb.PhaseMsg, prepare bool) (*pb.Ack, error) {
	if !n.auth.Verify(m.Sender, m) || m.View != n.view {
		return nil, status.Error(codes.PermissionDenied, "rejected")
	}
	n.mu.Lock()
	s := n.slot(m.Seq)
	if prepare {
		s.prep[m.Sender] = hex.EncodeToString(m.Digest)
	} else {
		s.comm[m.Sender] = hex.EncodeToString(m.Digest)
	}
	n.mu.Unlock()
	n.advance(m.Seq)
	return &pb.Ack{}, nil
}

func (n *Node) bcast(prepare bool, seq uint64, d []byte) {
	for p, c := range n.conns {
		go func(p string, c *grpc.ClientConn) {
			if n.inj.Drop() {
				return
			}
			m := &pb.PhaseMsg{View: n.view, Seq: seq, Digest: n.digestFor(p, d), Sender: n.id}
			n.auth.Sign(p, m)
			ctx, cancel := common.Ctx()
			defer cancel()
			cl := pb.NewPbftClient(c)
			if prepare {
				cl.Prepare(ctx, m)
			} else {
				cl.Commit(ctx, m)
			}
		}(p, c)
	}
}

// advance re-evaluates prepared/committed for one slot and executes in order.
func (n *Node) advance(seq uint64) {
	type ex struct {
		i  uint64
		op []byte
	}
	n.mu.Lock()
	s := n.slot(seq)
	sendCommit := false
	var d []byte
	if s.pre && !s.sentC {
		h, c := hex.EncodeToString(s.digest), 0
		for snd, dg := range s.prep {
			if snd != n.primary && dg == h {
				c++
			}
		}
		if c >= 2*n.f { // "prepared": pre-prepare + 2f matching prepares
			s.sentC, sendCommit, d = true, true, s.digest
			s.comm[n.id] = h
		}
	}
	if s.sentC && !s.done {
		h, c := hex.EncodeToString(s.digest), 0
		for _, dg := range s.comm {
			if dg == h {
				c++
			}
		}
		if c >= 2*n.f+1 {
			s.done = true // "committed-local"
		}
	}
	var out []ex
	for {
		x := n.slots[n.exec]
		if x == nil || !x.done {
			break
		}
		out = append(out, ex{n.exec, x.op})
		n.exec++
	}
	if len(out) > 0 {
		n.applyMu.Lock() // taken before mu is released => delivery order == pop order
	}
	n.mu.Unlock()
	if sendCommit {
		n.bcast(false, seq, d)
	}
	if len(out) > 0 {
		for _, e := range out {
			n.onCommit(e.i, e.op)
		}
		n.applyMu.Unlock()
	}
}

func (n *Node) Status() map[string]any {
	n.mu.Lock()
	defer n.mu.Unlock()
	role := "backup"
	if n.id == n.primary {
		role = "primary"
	}
	return map[string]any{"role": role, "leader": n.primary, "view": n.view, "commit": n.exec - 1}
}