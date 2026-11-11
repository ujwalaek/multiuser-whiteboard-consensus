package pbft

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
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
	checkpointEvery = 32                     // a checkpoint after every 32nd executed entry
	vcTimeout       = 1 * time.Second        // wait this long for NEW-VIEW before trying the next view
	resendAfter     = 250 * time.Millisecond // re-send own messages for a slot still waiting this long
)

// nullOp fills a sequence number the new primary cannot prove anything about; it commits as an empty batch.
var nullOp = []byte("[]")

type slot struct {
	view             uint64 // view of the pre-prepare accepted for this slot
	op, digest       []byte
	pre, sentC, done bool
	since            time.Time               // when this slot's pre-prepare was accepted (for re-sending)
	preMsg           *pb.PrePrepareMsg       // that signed pre-prepare, for prepared certificates
	prep             map[string]*pb.PhaseMsg // sender -> its signed prepare in this view
	comm             map[string]*pb.PhaseMsg // sender -> its commit (prepares and commits for a later view wait here)
	cert             *pb.PreparedCert        // highest-view prepared certificate held, kept across view changes
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
	evidence []common.Evidence
	Valid    func(op []byte) bool            // checks client signatures on a proposed entry (nil: accept all)
	seenPre  map[[2]uint64]*pb.PrePrepareMsg // (view, seq) -> first signed pre-prepare seen, for evidence
	accused  map[[2]uint64]bool

	state       []byte                                  // running hash of every executed digest
	stable      uint64                                  // last stable checkpoint
	stableProof []*pb.CheckpointMsg                     // the 2f+1 signed checkpoints proving it
	cps         map[uint64]map[string]*pb.CheckpointMsg // seq -> sender -> checkpoint
	changing    bool                                    // a view change is under way
	vcView      uint64                                  // the view it is moving to
	vcs         map[uint64]map[string]*pb.ViewChangeMsg // view -> sender -> view change
	vcTimer     *time.Timer
	early       map[uint64][]*pb.PrePrepareMsg // pre-prepares for a view not yet installed here
	history     map[uint64][]byte              // executed requests by seq, served to replicas catching up
	catching    bool                           // a state transfer is under way
	catchTo     uint64                         // ... and how far it must go
	lastNV      *pb.NewViewMsg                 // re-sent for a while after this replica announced a view
	nvUntil     time.Time
	done        chan struct{}
	views       int // view changes completed, for status
}

func New(id string, n int, conns map[string]*grpc.ClientConn, auth *common.Auth, inj *fault.Injector, cb common.CommitFn) *Node {
	return &Node{id: id, primary: "r0", n: n, f: (n - 1) / 3, exec: 1, slots: map[uint64]*slot{},
		conns: conns, auth: auth, inj: inj, onCommit: cb, state: make([]byte, 32),
		cps: map[uint64]map[string]*pb.CheckpointMsg{}, vcs: map[uint64]map[string]*pb.ViewChangeMsg{},
		early: map[uint64][]*pb.PrePrepareMsg{}, history: map[uint64][]byte{}, seenPre: map[[2]uint64]*pb.PrePrepareMsg{}, accused: map[[2]uint64]bool{},
		done: make(chan struct{})}
}

// Run starts re-sending: PBFT has no heartbeat, so a message lost for good (its retries
// timed out) would otherwise leave its slot waiting forever. Every message is idempotent.
func (n *Node) Run() {
	go func() {
		t := time.NewTicker(resendAfter)
		defer t.Stop()
		for {
			select {
			case <-n.done:
				return
			case <-t.C:
				n.resend()
			}
		}
	}()
}

// Stop ends re-sending.
func (n *Node) Stop() { close(n.done) }

func (n *Node) resend() {
	if n.inj.Mode() == fault.Crash {
		return
	}
	type pre struct {
		seq, view uint64
		op        []byte
	}
	var pres []pre
	var phases []*pb.PhaseMsg
	var commits []*pb.PhaseMsg
	n.mu.Lock()
	if !n.changing {
		for seq, s := range n.slots {
			if !s.pre || s.view != n.view || time.Since(s.since) < resendAfter || (s.done && seq < n.exec) {
				continue
			}
			if n.id == n.primary {
				pres = append(pres, pre{seq, s.view, s.op})
			} else if m := s.prep[n.id]; m != nil {
				phases = append(phases, m)
			}
			if m := s.comm[n.id]; m != nil && m.Sig != nil {
				commits = append(commits, m)
			}
		}
	}
	nv := n.lastNV
	if time.Now().After(n.nvUntil) {
		nv = nil
	}
	n.mu.Unlock()
	for _, p := range pres {
		n.sendPre(p.seq, p.view, p.op)
	}
	for _, m := range phases {
		n.bcast(true, m)
	}
	for _, m := range commits {
		n.bcast(false, m)
	}
	if nv != nil {
		n.each(func(_ string, cl pb.PbftClient) error { _, err := cl.NewView(context.Background(), nv); return err })
	}
}

var debug = os.Getenv("PBFTDEBUG") != ""

func dbg(f string, a ...any) {
	if debug {
		log.Printf(f, a...)
	}
}

func (n *Node) primaryOf(v uint64) string { return fmt.Sprintf("r%d", v%uint64(n.n)) }

// observe records m, a pre-prepare already verified as signed by the primary of its view,
// and stores evidence if that primary signed a different one for the same slot. It reports
// whether new evidence against the current primary was found. caller holds mu.
func (n *Node) observe(m *pb.PrePrepareMsg, reporter string) bool {
	k := [2]uint64{m.View, m.Seq}
	switch first := n.seenPre[k]; {
	case first == nil:
		n.seenPre[k] = m
	case !n.accused[k] && !bytes.Equal(first.Digest, m.Digest):
		n.accused[k] = true
		n.evidence = append(n.evidence, common.Evidence{Accused: m.Sender, Reporter: reporter, A: first, B: m})
		return m.View == n.view && !n.changing
	}
	return false
}

func (n *Node) Evidence() []common.Evidence {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]common.Evidence(nil), n.evidence...)
}

func (n *Node) slot(s uint64) *slot { // caller holds mu
	x := n.slots[s]
	if x == nil {
		x = &slot{prep: map[string]*pb.PhaseMsg{}, comm: map[string]*pb.PhaseMsg{}}
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
	n.mu.Lock()
	if n.id != n.primary || n.changing {
		p := n.primary
		n.mu.Unlock()
		return p, common.ErrNotLeader
	}
	if n.inj.Mode() == fault.Crash {
		n.mu.Unlock()
		return "", errors.New("crashed")
	}
	d := sha256.Sum256(op)
	n.seq++
	seq, view := n.seq, n.view
	s := n.slot(seq)
	s.view, s.op, s.digest, s.pre = view, op, d[:], true
	s.preMsg = &pb.PrePrepareMsg{View: view, Seq: seq, Digest: d[:], Op: op, Sender: n.id}
	n.auth.Sign(s.preMsg)
	s.since = time.Now()
	n.mu.Unlock()
	n.sendPre(seq, view, op)
	return n.id, nil
}

// sendPre sends the pre-prepare for seq to every backup (an equivocating primary sends
// odd-numbered backups an altered request).
func (n *Node) sendPre(seq, view uint64, op []byte) {
	d := sha256.Sum256(op)
	honest := &pb.PrePrepareMsg{View: view, Seq: seq, Digest: d[:], Op: op, Sender: n.id}
	n.auth.Sign(honest)
	n.each(func(p string, cl pb.PbftClient) error {
		m := honest
		if n.byz(p) {
			o := fault.Mutate(op)
			dd := sha256.Sum256(o)
			m = &pb.PrePrepareMsg{View: view, Seq: seq, Digest: dd[:], Op: o, Sender: n.id}
			n.auth.Sign(m)
		}
		_, err := cl.PrePrepare(context.Background(), m) // send replaces the context
		return err
	})
}

func (n *Node) PrePrepare(_ context.Context, m *pb.PrePrepareMsg) (*pb.Ack, error) {
	if !n.auth.Verify(m.Sender, m) {
		return nil, status.Error(codes.PermissionDenied, "rejected")
	}
	if d := sha256.Sum256(m.Op); !bytes.Equal(d[:], m.Digest) {
		return nil, status.Error(codes.InvalidArgument, "digest mismatch")
	}
	valid := n.Valid == nil || n.Valid(m.Op)
	n.mu.Lock()
	if m.View > n.view && m.View <= n.view+uint64(n.n) && m.Sender == n.primaryOf(m.View) {
		// the new primary is already in its view and this replica is not, yet: keep it for install
		n.early[m.View] = append(n.early[m.View], m)
		n.mu.Unlock()
		return &pb.Ack{}, nil
	}
	if m.Sender != n.primary || m.View != n.view || n.changing || m.Seq <= n.stable {
		n.mu.Unlock()
		return nil, status.Error(codes.PermissionDenied, "rejected")
	}
	if !valid { // the primary signed a request no client signed: it is faulty
		n.observe(m, n.id)
		n.mu.Unlock()
		go n.Suspect()
		return nil, status.Error(codes.InvalidArgument, "request not signed by its client")
	}
	s := n.slot(m.Seq)
	if s.pre && s.view == m.View && !bytes.Equal(s.digest, m.Digest) {
		accuse := n.observe(m, n.id)
		n.mu.Unlock()
		n.suspectIf(accuse)
		return nil, status.Error(codes.AlreadyExists, "conflicting pre-prepare") // primary equivocation detected
	}
	own := n.accept(m)
	accuse := n.observe(m, n.id)
	n.mu.Unlock()
	n.suspectIf(accuse)
	n.bcast(true, own)
	n.advance(m.Seq)
	return &pb.Ack{}, nil
}

// accept takes m as this slot's pre-prepare and returns this backup's own signed prepare for it. caller holds mu.
func (n *Node) accept(m *pb.PrePrepareMsg) *pb.PhaseMsg {
	s := n.slot(m.Seq)
	keepView(s.prep, m.View)
	keepView(s.comm, m.View)
	if s.view != m.View {
		s.sentC = false // a re-issued slot is prepared and committed again in the new view
	}
	s.view, s.op, s.digest, s.pre, s.preMsg, s.since = m.View, m.Op, m.Digest, true, m, time.Now()
	own := &pb.PhaseMsg{View: m.View, Seq: m.Seq, Digest: m.Digest, Sender: n.id, Proof: m}
	n.auth.Sign(own)
	s.prep[n.id] = own
	return own
}

func (n *Node) Prepare(_ context.Context, m *pb.PhaseMsg) (*pb.Ack, error) { return n.phase(m, true) }
func (n *Node) Commit(_ context.Context, m *pb.PhaseMsg) (*pb.Ack, error)  { return n.phase(m, false) }

func (n *Node) phase(m *pb.PhaseMsg, prepare bool) (*pb.Ack, error) {
	if !n.auth.Verify(m.Sender, m) {
		return nil, status.Error(codes.PermissionDenied, "rejected")
	}
	pf := m.Proof // a forwarded pre-prepare counts as evidence only if that view's primary really signed it
	okProof := prepare && pf != nil && pf.View == m.View && pf.Seq == m.Seq && pf.Sender == n.primaryOf(pf.View) &&
		n.auth.Verify(pf.Sender, pf)
	n.mu.Lock()
	accuse := okProof && n.observe(pf, m.Sender) // proof about an earlier view still counts
	// Messages for a later view are kept: a replica that installs the new view a little
	// after its peers must not lose the prepares they already sent in it.
	if m.View < n.view || m.View > n.view+uint64(n.n) || m.Seq <= n.stable {
		n.mu.Unlock()
		n.suspectIf(accuse)
		return nil, status.Error(codes.PermissionDenied, "rejected")
	}
	s := n.slot(m.Seq)
	if prepare {
		s.prep[m.Sender] = m
	} else {
		s.comm[m.Sender] = m
	}
	n.mu.Unlock()
	n.suspectIf(accuse)
	n.advance(m.Seq)
	return &pb.Ack{}, nil
}

// bcast sends a prepare (carrying proof, the primary's signed pre-prepare) or a commit to every peer.
// own is this replica's honest signed message, sent as is unless it is lying to that peer.
// It takes no lock: advance calls it while holding applyMu.
func (n *Node) bcast(prepare bool, own *pb.PhaseMsg) {
	n.each(func(p string, cl pb.PbftClient) error {
		m := own
		if n.byz(p) {
			m = &pb.PhaseMsg{View: own.View, Seq: own.Seq, Digest: n.digestFor(p, own.Digest), Sender: n.id, Proof: own.Proof}
			n.auth.Sign(m)
		}
		var err error
		if prepare {
			_, err = cl.Prepare(context.Background(), m)
		} else {
			_, err = cl.Commit(context.Background(), m)
		}
		return err
	})
}

// each calls rpc once per peer, in parallel, skipping peers the fault injector cuts off.
func (n *Node) each(rpc func(p string, cl pb.PbftClient) error) {
	for p, c := range n.conns {
		go func(p string, c *grpc.ClientConn) {
			if n.inj.Drop(p) {
				return
			}
			cl := pb.NewPbftClient(c)
			send(func(ctx context.Context) error { return rpc(p, ctxClient{cl, ctx}) })
		}(p, c)
	}
}

// ctxClient makes every call through it use ctx, so rpc bodies need not thread a context.
type ctxClient struct {
	pb.PbftClient
	ctx context.Context
}

func (c ctxClient) PrePrepare(_ context.Context, m *pb.PrePrepareMsg, o ...grpc.CallOption) (*pb.Ack, error) {
	return c.PbftClient.PrePrepare(c.ctx, m, o...)
}
func (c ctxClient) Prepare(_ context.Context, m *pb.PhaseMsg, o ...grpc.CallOption) (*pb.Ack, error) {
	return c.PbftClient.Prepare(c.ctx, m, o...)
}
func (c ctxClient) Commit(_ context.Context, m *pb.PhaseMsg, o ...grpc.CallOption) (*pb.Ack, error) {
	return c.PbftClient.Commit(c.ctx, m, o...)
}
func (c ctxClient) Checkpoint(_ context.Context, m *pb.CheckpointMsg, o ...grpc.CallOption) (*pb.Ack, error) {
	return c.PbftClient.Checkpoint(c.ctx, m, o...)
}
func (c ctxClient) ViewChange(_ context.Context, m *pb.ViewChangeMsg, o ...grpc.CallOption) (*pb.Ack, error) {
	return c.PbftClient.ViewChange(c.ctx, m, o...)
}
func (c ctxClient) NewView(_ context.Context, m *pb.NewViewMsg, o ...grpc.CallOption) (*pb.Ack, error) {
	return c.PbftClient.NewView(c.ctx, m, o...)
}

// send makes one RPC, retrying a few times if it timed out or the peer was unreachable.
// PBFT has no heartbeats to repair a lost message (Raft's AppendEntries does), so without
// this one slow connection setup can stall a slot forever. A peer's deliberate rejection
// is not retried, and the fault injector's drops happen before send, so they stay dropped.
func send(call func(ctx context.Context) error) {
	for attempt := 0; attempt < 3; attempt++ {
		ctx, cancel := common.Ctx()
		err := call(ctx)
		cancel()
		if c := status.Code(err); c != codes.DeadlineExceeded && c != codes.Unavailable {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// keepCert records s's prepared certificate if it is prepared in a view later than the
// certificate already kept. A replica must report, in every later view change, each
// request it ever prepared (the highest view's), or a new primary could re-issue a
// null request for a slot that committed elsewhere. caller holds mu.
func (n *Node) keepCert(s *slot) {
	if !n.prepared(s) || (s.cert != nil && s.cert.Pre.View >= s.view) {
		return
	}
	c := &pb.PreparedCert{Pre: s.preMsg}
	for snd, p := range s.prep {
		if snd != n.primaryOf(s.view) && p.View == s.view && bytes.Equal(p.Digest, s.digest) {
			c.Prepares = append(c.Prepares, p)
		}
	}
	s.cert = c
}

// prepared reports whether s holds its pre-prepare plus 2f matching prepares from backups. caller holds mu.
func (n *Node) prepared(s *slot) bool {
	if !s.pre {
		return false
	}
	h, c := hex.EncodeToString(s.digest), 0
	for snd, m := range s.prep {
		if snd != n.primaryOf(s.view) && m.View == s.view && hex.EncodeToString(m.Digest) == h {
			c++
		}
	}
	return c >= 2*n.f
}

// advance re-evaluates prepared/committed for one slot and executes in order.
func (n *Node) advance(seq uint64) {
	type ex struct {
		i  uint64
		op []byte
	}
	n.mu.Lock()
	s := n.slot(seq)
	var commit *pb.PhaseMsg
	if !s.sentC && !n.changing && s.view == n.view && n.prepared(s) {
		s.sentC = true
		commit = &pb.PhaseMsg{View: s.view, Seq: seq, Digest: s.digest, Sender: n.id}
		n.auth.Sign(commit)
		s.comm[n.id] = commit
	}
	if s.sentC && !s.done {
		c := 0
		for _, m := range s.comm {
			if m.View == s.view && bytes.Equal(m.Digest, s.digest) {
				c++
			}
		}
		if c >= 2*n.f+1 {
			s.done = true // "committed-local"
			dbg("%s DONE seq=%d view=%d nview=%d changing=%v op=%.20s", n.id, seq, s.view, n.view, n.changing, s.op)
		}
	}
	var out []ex
	var cps []*pb.CheckpointMsg
	for {
		x := n.slots[n.exec]
		if x == nil || !x.done {
			break
		}
		out = append(out, ex{n.exec, x.op})
		n.history[n.exec] = x.op
		h := sha256.Sum256(append(append([]byte{}, n.state...), x.digest...))
		n.state = h[:]
		if n.exec%checkpointEvery == 0 {
			cp := &pb.CheckpointMsg{Seq: n.exec, Digest: n.state, Sender: n.id}
			n.auth.Sign(cp)
			cps = append(cps, cp)
		}
		n.exec++
	}
	for _, cp := range cps {
		n.addCheckpoint(cp)
	}
	if len(out) > 0 {
		n.applyMu.Lock() // taken before mu is released => delivery order == pop order
	}
	n.mu.Unlock()
	if commit != nil {
		n.bcast(false, commit)
	}
	if len(out) > 0 {
		for _, e := range out {
			n.onCommit(e.i, e.op)
		}
		n.applyMu.Unlock()
	}
	for _, cp := range cps {
		n.each(func(_ string, cl pb.PbftClient) error { _, err := cl.Checkpoint(context.Background(), cp); return err })
	}
}

// ---- checkpoints ----

func (n *Node) Checkpoint(_ context.Context, m *pb.CheckpointMsg) (*pb.Ack, error) {
	if !n.auth.Verify(m.Sender, m) {
		return nil, status.Error(codes.PermissionDenied, "rejected")
	}
	n.mu.Lock()
	n.addCheckpoint(m)
	n.mu.Unlock()
	return &pb.Ack{}, nil
}

// addCheckpoint records m and, once 2f+1 replicas report the same state at a seq, makes it
// stable: slots up to it are dropped and later view changes start above it. caller holds mu.
func (n *Node) addCheckpoint(m *pb.CheckpointMsg) {
	if m.Seq <= n.stable {
		return
	}
	if n.cps[m.Seq] == nil {
		n.cps[m.Seq] = map[string]*pb.CheckpointMsg{}
	}
	n.cps[m.Seq][m.Sender] = m
	var match []*pb.CheckpointMsg
	for _, c := range n.cps[m.Seq] {
		if bytes.Equal(c.Digest, m.Digest) {
			match = append(match, c)
		}
	}
	if len(match) >= 2*n.f+1 && m.Seq >= n.exec { // 2f+1 replicas are past us: fetch what we missed
		go n.catchUp(m.Seq)
	}
	if len(match) < 2*n.f+1 || m.Seq >= n.exec { // never mark stable what this replica has not executed
		return
	}
	n.stable, n.stableProof = m.Seq, match
	for s := range n.slots {
		if s <= m.Seq {
			delete(n.slots, s)
		}
	}
	for s := range n.cps {
		if s <= m.Seq {
			delete(n.cps, s)
		}
	}
}

// ---- view change ----

// Suspect starts a view change unless one is already under way: the server calls it when a
// request it watched was not executed in time, which is how a crashed or silent primary is noticed.
func (n *Node) Suspect() {
	n.mu.Lock()
	v, busy := n.view+1, n.changing
	n.mu.Unlock()
	if !busy {
		n.startViewChange(v)
	}
}

func (n *Node) suspectIf(b bool) {
	if b { // signed proof that the primary equivocated: no reason to wait for a timeout
		go n.Suspect()
	}
}

// startViewChange stops accepting messages in the current view and asks to move to view v,
// sending this replica's stable checkpoint and every prepared certificate above it.
func (n *Node) startViewChange(v uint64) {
	n.mu.Lock()
	if v <= n.view || (n.changing && v <= n.vcView) || n.inj.Mode() == fault.Crash {
		n.mu.Unlock()
		return
	}
	n.changing, n.vcView = true, v
	m := &pb.ViewChangeMsg{View: v, StableSeq: n.stable, Checkpoint: n.stableProof, Sender: n.id}
	for seq, s := range n.slots {
		if seq <= n.stable {
			continue
		}
		n.keepCert(s)
		if s.cert != nil {
			m.Prepared = append(m.Prepared, s.cert)
		}
	}
	sort.Slice(m.Prepared, func(i, j int) bool { return m.Prepared[i].Pre.Seq < m.Prepared[j].Pre.Seq })
	var cs []uint64
	for _, c := range m.Prepared {
		cs = append(cs, c.Pre.Seq)
	}
	dbg("%s VC v=%d stable=%d exec=%d certs=%v", n.id, v, n.stable, n.exec, cs)
	n.auth.Sign(m)
	n.addVC(m)
	if n.vcTimer != nil {
		n.vcTimer.Stop()
	}
	wait := vcTimeout << min(v-n.view-1, 4)   // doubles with each view tried in a row, as in PBFT
	n.vcTimer = time.AfterFunc(wait, func() { // no new view in time: that primary is faulty too
		n.mu.Lock()
		stuck := n.changing && n.vcView == v
		n.mu.Unlock()
		if stuck {
			n.startViewChange(v + 1)
		}
	})
	n.mu.Unlock()
	n.each(func(_ string, cl pb.PbftClient) error { _, err := cl.ViewChange(context.Background(), m); return err })
	n.tryNewView(v)
}

func (n *Node) addVC(m *pb.ViewChangeMsg) { // caller holds mu
	if n.vcs[m.View] == nil {
		n.vcs[m.View] = map[string]*pb.ViewChangeMsg{}
	}
	n.vcs[m.View][m.Sender] = m
}

func (n *Node) ViewChange(_ context.Context, m *pb.ViewChangeMsg) (*pb.Ack, error) {
	if !n.auth.Verify(m.Sender, m) || !n.validVC(m) {
		return nil, status.Error(codes.PermissionDenied, "rejected")
	}
	n.mu.Lock()
	if m.View <= n.view {
		n.mu.Unlock()
		return &pb.Ack{}, nil
	}
	n.addVC(m)
	// f+1 replicas asking for a later view include an honest one: join the lowest such view.
	target := n.view
	if n.changing {
		target = n.vcView
	}
	senders, lowest := map[string]bool{}, uint64(0)
	for v, byS := range n.vcs {
		if v <= target {
			continue
		}
		for s := range byS {
			senders[s] = true
		}
		if lowest == 0 || v < lowest {
			lowest = v
		}
	}
	join := len(senders) >= n.f+1
	n.mu.Unlock()
	if join {
		n.startViewChange(lowest)
	}
	n.tryNewView(m.View)
	return &pb.Ack{}, nil
}

// validVC checks a view change's checkpoint proof and every prepared certificate in it.
func (n *Node) validVC(m *pb.ViewChangeMsg) bool {
	if m.StableSeq > 0 {
		who := map[string]bool{}
		for _, c := range m.Checkpoint {
			if c.Seq == m.StableSeq && bytes.Equal(c.Digest, m.Checkpoint[0].Digest) && n.auth.Verify(c.Sender, c) {
				who[c.Sender] = true
			}
		}
		if len(who) < 2*n.f+1 {
			return false
		}
	}
	for _, c := range m.Prepared {
		pre := c.Pre
		if pre == nil || pre.Seq <= m.StableSeq || pre.View >= m.View || pre.Sender != n.primaryOf(pre.View) ||
			!n.auth.Verify(pre.Sender, pre) {
			return false
		}
		if d := sha256.Sum256(pre.Op); !bytes.Equal(d[:], pre.Digest) {
			return false
		}
		who := map[string]bool{}
		for _, p := range c.Prepares {
			if p.Sender != pre.Sender && p.View == pre.View && p.Seq == pre.Seq && bytes.Equal(p.Digest, pre.Digest) &&
				n.auth.Verify(p.Sender, p) {
				who[p.Sender] = true
			}
		}
		if len(who) < 2*n.f {
			return false
		}
	}
	return true
}

// reissue computes, from 2f+1 view changes, where the new view starts (the highest stable
// checkpoint) and what goes in each later slot: the request with the highest-view prepared
// certificate, or a null request if no replica in the set prepared anything there.
func (n *Node) reissue(vcs []*pb.ViewChangeMsg) (minS uint64, ops map[uint64][]byte) {
	best, maxS := map[uint64]*pb.PrePrepareMsg{}, uint64(0)
	for _, m := range vcs {
		if m.StableSeq > minS {
			minS = m.StableSeq
		}
		for _, c := range m.Prepared {
			if b := best[c.Pre.Seq]; b == nil || c.Pre.View > b.View {
				best[c.Pre.Seq] = c.Pre
			}
			if c.Pre.Seq > maxS {
				maxS = c.Pre.Seq
			}
		}
	}
	ops = map[uint64][]byte{}
	for s := minS + 1; s <= maxS; s++ {
		ops[s] = nullOp
		if b := best[s]; b != nil {
			ops[s] = b.Op
		}
	}
	return minS, ops
}

// tryNewView: the primary of view v, holding 2f+1 view changes for it, announces the view.
func (n *Node) tryNewView(v uint64) {
	n.mu.Lock()
	if n.primaryOf(v) != n.id || v <= n.view || !n.changing || n.vcView != v || len(n.vcs[v]) < 2*n.f+1 {
		n.mu.Unlock()
		return
	}
	var senders []string
	for s := range n.vcs[v] {
		senders = append(senders, s)
	}
	sort.Strings(senders)
	nv := &pb.NewViewMsg{View: v, Sender: n.id}
	for _, s := range senders[:2*n.f+1] {
		nv.ViewChanges = append(nv.ViewChanges, n.vcs[v][s])
	}
	minS, ops := n.reissue(nv.ViewChanges)
	dbg("%s NEWVIEW v=%d from %v minS=%d ops=%d", n.id, v, senders[:2*n.f+1], minS, len(ops))
	for s := minS + 1; s <= minS+uint64(len(ops)); s++ {
		d := sha256.Sum256(ops[s])
		pre := &pb.PrePrepareMsg{View: v, Seq: s, Digest: d[:], Op: ops[s], Sender: n.id}
		n.auth.Sign(pre)
		nv.PrePrepares = append(nv.PrePrepares, pre)
	}
	n.auth.Sign(nv)
	n.lastNV, n.nvUntil = nv, time.Now().Add(3*time.Second)
	n.mu.Unlock()
	n.each(func(_ string, cl pb.PbftClient) error { _, err := cl.NewView(context.Background(), nv); return err })
	n.install(nv, minS)
}

func (n *Node) NewView(_ context.Context, m *pb.NewViewMsg) (*pb.Ack, error) {
	if !n.auth.Verify(m.Sender, m) || m.Sender != n.primaryOf(m.View) {
		return nil, status.Error(codes.PermissionDenied, "rejected")
	}
	who := map[string]bool{}
	for _, vc := range m.ViewChanges {
		if vc.View != m.View || !n.auth.Verify(vc.Sender, vc) || !n.validVC(vc) {
			return nil, status.Error(codes.PermissionDenied, "bad view change in new view")
		}
		who[vc.Sender] = true
	}
	if len(who) < 2*n.f+1 {
		return nil, status.Error(codes.PermissionDenied, "too few view changes")
	}
	// recompute what the primary had to re-issue; anything else is a lie
	minS, ops := n.reissue(m.ViewChanges)
	if len(m.PrePrepares) != len(ops) {
		return nil, status.Error(codes.PermissionDenied, "wrong re-issued pre-prepares")
	}
	for _, p := range m.PrePrepares {
		want, ok := ops[p.Seq]
		if !ok || p.View != m.View || p.Sender != m.Sender || !bytes.Equal(p.Op, want) || !n.auth.Verify(p.Sender, p) {
			return nil, status.Error(codes.PermissionDenied, "wrong re-issued pre-prepares")
		}
	}
	n.mu.Lock()
	stale := m.View <= n.view
	n.mu.Unlock()
	if !stale {
		n.install(m, minS)
	}
	return &pb.Ack{}, nil
}

// install moves this replica into the new view: slots not yet committed are cleared and the
// re-issued pre-prepares are processed as if the new primary had just sent them.
func (n *Node) install(m *pb.NewViewMsg, minS uint64) {
	n.mu.Lock()
	if m.View <= n.view {
		n.mu.Unlock()
		return
	}
	n.view, n.primary, n.changing, n.views = m.View, n.primaryOf(m.View), false, n.views+1
	if n.vcTimer != nil {
		n.vcTimer.Stop()
	}
	for v := range n.vcs {
		if v <= m.View {
			delete(n.vcs, v)
		}
	}
	for _, s := range n.slots {
		n.keepCert(s) // what was prepared must be reported in any later view change
		if !s.done {
			s.pre, s.sentC, s.preMsg = false, false, nil
			keepView(s.prep, m.View)
			keepView(s.comm, m.View)
		}
	}
	hi := n.exec - 1
	if minS > hi {
		hi = minS
	}
	type out struct {
		own *pb.PhaseMsg
		pre *pb.PrePrepareMsg
	}
	var sends []out
	for _, pre := range m.PrePrepares {
		if pre.Seq > hi {
			hi = pre.Seq
		}
		if pre.Seq <= n.stable {
			continue // dropped here after a stable checkpoint; others hold it too
		}
		if s := n.slots[pre.Seq]; s != nil && s.done && !bytes.Equal(s.digest, pre.Digest) {
			log.Printf("pbft %s: new view %d re-issues a different request at committed seq %d; ignored", n.id, m.View, pre.Seq)
			continue // cannot happen while 2f+1 replicas are honest; never overwrite a commit
		}
		// A slot already committed here is still prepared and committed again in the new
		// view: a replica that fell behind needs these votes to reach 2f+1. Its request is
		// the same one (it was committed), and executing stays exactly once.
		if n.id == n.primary {
			s := n.slot(pre.Seq)
			if s.view != pre.View {
				s.sentC = false
			}
			s.view, s.op, s.digest, s.pre, s.preMsg, s.since = pre.View, pre.Op, pre.Digest, true, pre, time.Now()
		} else {
			sends = append(sends, out{n.accept(pre), pre})
		}
	}
	if minS >= n.exec { // the new view starts past what this replica executed
		go n.catchUp(minS)
	}
	if n.id == n.primary {
		n.seq = hi
	}
	early := n.early[m.View]
	for v := range n.early {
		if v <= m.View {
			delete(n.early, v)
		}
	}
	n.mu.Unlock()
	for _, o := range sends {
		n.bcast(true, o.own)
	}
	for _, pre := range m.PrePrepares {
		n.advance(pre.Seq)
	}
	for _, pre := range early {
		n.PrePrepare(context.Background(), pre)
	}
}

// ---- state transfer ----

// Fetch returns the requests this replica executed in [from, to].
func (n *Node) Fetch(_ context.Context, r *pb.FetchReq) (*pb.FetchResp, error) {
	if !n.auth.Verify(r.Sender, r) {
		return nil, status.Error(codes.PermissionDenied, "rejected")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	resp := &pb.FetchResp{}
	for s := r.From; s <= r.To && s < r.From+1024; s++ {
		op, ok := n.history[s]
		if !ok {
			break
		}
		d := sha256.Sum256(op)
		resp.Entries = append(resp.Entries, &pb.PrePrepareMsg{Seq: s, Op: op, Digest: d[:]})
	}
	return resp, nil
}

// catchUp brings a replica that fell behind up to seq to: it asks every peer for the
// committed requests it is missing and takes each one that f+1 peers return identically,
// which includes at least one honest replica. They are then executed in order as usual.
func (n *Node) catchUp(to uint64) {
	n.mu.Lock()
	n.catchTo = max(n.catchTo, to)
	if n.catching || n.exec > to {
		n.mu.Unlock()
		return // a running catch-up picks up the new target
	}
	n.catching = true
	n.mu.Unlock()
	defer func() { n.mu.Lock(); n.catching = false; n.mu.Unlock() }()
	for try := 0; try < 10; try++ { // peers may be busy: a fetch can time out
		n.mu.Lock()
		from, to := n.exec, n.catchTo
		n.mu.Unlock()
		if from > to {
			return
		}
		n.fetch(from, to)
		time.Sleep(100 * time.Millisecond)
	}
}

func (n *Node) fetch(from, to uint64) {

	req := &pb.FetchReq{From: from, To: to, Sender: n.id}
	n.auth.Sign(req)
	var mu sync.Mutex
	votes := map[uint64]map[string]map[string]bool{} // seq -> digest -> peers
	ops := map[string][]byte{}
	var wg sync.WaitGroup
	for p, c := range n.conns {
		if n.inj.Drop(p) {
			continue
		}
		wg.Add(1)
		go func(p string, c *grpc.ClientConn) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			resp, err := pb.NewPbftClient(c).Fetch(ctx, req)
			if err != nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			for _, e := range resp.Entries {
				if d := sha256.Sum256(e.Op); !bytes.Equal(d[:], e.Digest) {
					continue
				}
				k := hex.EncodeToString(e.Digest)
				if votes[e.Seq] == nil {
					votes[e.Seq] = map[string]map[string]bool{}
				}
				if votes[e.Seq][k] == nil {
					votes[e.Seq][k] = map[string]bool{}
				}
				votes[e.Seq][k][p], ops[k] = true, e.Op
			}
		}(p, c)
	}
	wg.Wait()
	n.mu.Lock()
	for seq := from; seq <= to; seq++ {
		var op []byte
		for k, who := range votes[seq] {
			if len(who) >= n.f+1 {
				op = ops[k]
			}
		}
		if op == nil {
			break
		}
		s := n.slot(seq)
		if !s.done {
			d := sha256.Sum256(op)
			s.op, s.digest, s.pre, s.done = op, d[:], true, true
		}
	}
	n.mu.Unlock()
	n.advance(from)
}

// keepView drops the messages of earlier views.
func keepView(ms map[string]*pb.PhaseMsg, v uint64) {
	for k, m := range ms {
		if m.View < v {
			delete(ms, k)
		}
	}
}

func (n *Node) Leader() (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.primary, n.id == n.primary && !n.changing && n.inj.Mode() != fault.Crash
}

func (n *Node) Status() map[string]any {
	n.mu.Lock()
	defer n.mu.Unlock()
	role := "backup"
	if n.id == n.primary {
		role = "primary"
	}
	return map[string]any{"role": role, "leader": n.primary, "view": n.view, "commit": n.exec - 1,
		"changing": n.changing, "stable": n.stable, "view_changes": n.views}
}
