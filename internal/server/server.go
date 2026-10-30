package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"wb/internal/common"
	"wb/internal/fault"
)

type Server struct {
	ID, Proto string
	N         int // cluster size, reported to clients so they know f
	Cons      common.Consensus
	Inj       *fault.Injector
	BatchMax  int           // most ops per consensus entry
	BatchWait time.Duration // longest the leader holds an op waiting for others
	mu        sync.Mutex
	log       []Block
	seen      map[string]bool // client/id of every applied request, for deduplication
	subs      map[*websocket.Conn]bool
	batch     *batcher
}

func (s *Server) Init() {
	s.subs, s.seen = map[*websocket.Conn]bool{}, map[string]bool{}
	s.batch = newBatcher(s.BatchMax, s.BatchWait, func(entry []byte) error {
		_, err := s.Cons.Propose(entry)
		return err
	})
}

func (s *Server) head() string { // caller holds mu
	if len(s.log) == 0 {
		return genesis
	}
	return s.log[len(s.log)-1].Hash
}

// OnCommit is the callback consensus calls (in order) for every committed entry.
// Every committed op is chained into the ledger; a retried request (same client/id)
// is chained too but marked Dup and not drawn, so all replicas skip the same ones.
func (s *Server) OnCommit(i uint64, entry []byte) {
	ops := decodeBatch(entry)
	s.mu.Lock()
	defer s.mu.Unlock()
	b := Block{Index: i, Ops: ops, Dup: make([]bool, len(ops))}
	for k, op := range ops {
		var req struct{ Client, ID string }
		json.Unmarshal(op, &req)
		if key := req.Client + "/" + req.ID; req.ID != "" {
			b.Dup[k] = s.seen[key]
			s.seen[key] = true
		}
	}
	seal(&b, s.head())
	s.log = append(s.log, b)
	out := b
	if s.Inj.Equivocating() { // a Byzantine replica lies to its own clients too
		out = lie(b)
	}
	for c := range s.subs {
		c.SetWriteDeadline(time.Now().Add(time.Second))
		if c.WriteJSON(out) != nil {
			delete(s.subs, c)
			c.Close()
		}
	}
}

// lie returns a tampered but internally consistent copy of b: every op altered,
// Merkle root and hash recomputed, so only comparison with other replicas exposes it.
func lie(b Block) Block {
	t := b
	t.Ops = make([]json.RawMessage, len(b.Ops))
	for k, op := range b.Ops {
		t.Ops[k] = fault.Mutate(op)
	}
	seal(&t, b.Prev)
	return t
}

var up = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.Dir("web")))
	mux.HandleFunc("/ws", s.ws)
	mux.HandleFunc("/submit", s.submit)
	mux.HandleFunc("/admin/status", s.status)
	mux.HandleFunc("/admin/log", s.logH)
	mux.HandleFunc("/admin/fault", s.faultH)
	mux.HandleFunc("/admin/evidence", s.evidenceH)
	return cors(mux)
}

func cors(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			return
		}
		h.ServeHTTP(w, r)
	})
}

func (s *Server) ws(w http.ResponseWriter, r *http.Request) {
	c, err := up.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	s.mu.Lock()
	for _, b := range s.log { // replay so late joiners converge
		if s.Inj.Equivocating() {
			b = lie(b)
		}
		c.WriteJSON(b)
	}
	s.subs[c] = true
	s.mu.Unlock()
	for {
		if _, _, err := c.ReadMessage(); err != nil {
			break
		}
	}
	s.mu.Lock()
	delete(s.subs, c)
	s.mu.Unlock()
	c.Close()
}

// POST /submit: the leader/primary batches the op into the next consensus entry and
// answers once it is proposed; any other replica redirects to the one it believes leads.
func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	if !json.Valid(body) {
		http.Error(w, `{"error":"bad json"}`, 400)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	hint, self := s.Cons.Leader()
	switch {
	case self && s.batch.submit(body) == nil:
		w.Write([]byte(`{"ok":true}`))
	case !self && hint != "":
		fmt.Fprintf(w, `{"redirect":"http://%s:%d"}`, hostOf(r), 8000+common.Num(hint))
	default:
		w.WriteHeader(503)
		w.Write([]byte(`{"error":"no leader available"}`))
	}
}

func hostOf(r *http.Request) string {
	h := r.Host
	for i := len(h) - 1; i >= 0; i-- {
		if h[i] == ':' {
			return h[:i]
		}
	}
	return h
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	st := s.Cons.Status()
	st["id"], st["proto"], st["fault"] = s.ID, s.Proto, string(s.Inj.Mode())
	st["n"], st["f"] = s.N, (s.N-1)/3
	if s.Proto == "raft" {
		st["f"] = (s.N - 1) / 2
	}
	st["sent"], st["sent_bytes"] = common.Sent.Load(), common.SentBytes.Load()
	s.mu.Lock()
	st["applied"] = len(s.log)
	ops := 0
	for _, b := range s.log {
		ops += len(b.Ops)
	}
	st["applied_ops"] = ops
	st["head"] = s.head()
	s.mu.Unlock()
	json.NewEncoder(w).Encode(st)
}

// GET /admin/log: the ledger as this replica presents it (a Byzantine one lies here too).
func (s *Server) logH(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.log
	if s.Inj.Equivocating() {
		out = make([]Block, len(s.log))
		for i, b := range s.log {
			out[i] = lie(b)
		}
	}
	json.NewEncoder(w).Encode(out)
}

// GET /admin/evidence: signed proof of equivocation this replica has collected ([] for Raft).
func (s *Server) evidenceH(w http.ResponseWriter, r *http.Request) {
	ev := []common.Evidence{}
	if src, ok := s.Cons.(common.EvidenceSource); ok {
		ev = append(ev, src.Evidence()...)
	}
	json.NewEncoder(w).Encode(ev)
}

// POST /admin/fault?mode=none|crash|omit|equivocate&p=0.3
// POST /admin/fault?mode=partition&peers=r0,r1,r4   (the replicas this one can still reach)
// POST /admin/fault?delay=25ms                      (one-way delay on everything it sends)
func (s *Server) faultH(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if r.Method == http.MethodPost {
		switch m := fault.Mode(q.Get("mode")); {
		case m == fault.Partition:
			s.Inj.SetPartition(strings.Split(q.Get("peers"), ","))
		case m != "":
			p, _ := strconv.ParseFloat(q.Get("p"), 64)
			s.Inj.Set(m, p)
		}
		if d, err := time.ParseDuration(q.Get("delay")); err == nil {
			s.Inj.SetDelay(d)
		}
	}
	json.NewEncoder(w).Encode(map[string]any{"mode": s.Inj.Mode(), "p": s.Inj.P(),
		"peers": s.Inj.Peers(), "delay": s.Inj.Delay().String()})
}
