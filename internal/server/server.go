package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"wb/internal/common"
	"wb/internal/fault"
)

type Server struct {
	ID, Proto string
	Cons      common.Consensus
	Inj       *fault.Injector
	mu        sync.Mutex
	log       []Block
	seen      map[string]bool // client/id of every applied request, for deduplication
	subs      map[*websocket.Conn]bool
}

func (s *Server) Init() { s.subs, s.seen = map[*websocket.Conn]bool{}, map[string]bool{} }

func (s *Server) head() string { // caller holds mu
	if len(s.log) == 0 {
		return genesis
	}
	return s.log[len(s.log)-1].Hash
}

// OnCommit is the callback consensus calls (in order) for every committed op.
// Every committed op is chained into the ledger; a retried request (same client/id)
// is chained too but marked Dup and not drawn, so all replicas skip the same ones.
func (s *Server) OnCommit(i uint64, op []byte) {
	if !json.Valid(op) {
		return
	}
	var req struct{ Client, ID string }
	json.Unmarshal(op, &req)
	s.mu.Lock()
	defer s.mu.Unlock()
	prev := s.head()
	e := Block{Index: i, Op: json.RawMessage(op), Prev: prev, Hash: chain(prev, i, op)}
	if key := req.Client + "/" + req.ID; req.ID != "" {
		e.Dup = s.seen[key]
		s.seen[key] = true
	}
	s.log = append(s.log, e)
	for c := range s.subs {
		c.SetWriteDeadline(time.Now().Add(time.Second))
		if c.WriteJSON(e) != nil {
			delete(s.subs, c)
			c.Close()
		}
	}
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
	for _, e := range s.log { // replay so late joiners converge
		c.WriteJSON(e)
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

func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	if !json.Valid(body) {
		http.Error(w, `{"error":"bad json"}`, 400)
		return
	}
	hint, err := s.Cons.Propose(body)
	w.Header().Set("Content-Type", "application/json")
	switch {
	case err == nil:
		w.Write([]byte(`{"ok":true}`))
	case errors.Is(err, common.ErrNotLeader) && hint != "":
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
	s.mu.Lock()
	st["applied"] = len(s.log)
	st["head"] = s.head()
	s.mu.Unlock()
	json.NewEncoder(w).Encode(st)
}

func (s *Server) logH(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	json.NewEncoder(w).Encode(s.log)
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
func (s *Server) faultH(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		p, _ := strconv.ParseFloat(r.URL.Query().Get("p"), 64)
		s.Inj.Set(fault.Mode(r.URL.Query().Get("mode")), p)
	}
	fmt.Fprintf(w, `{"mode":%q,"p":%v}`, s.Inj.Mode(), s.Inj.P())
}