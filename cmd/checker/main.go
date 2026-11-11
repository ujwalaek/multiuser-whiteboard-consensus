package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"wb/internal/common"
	"wb/internal/server"
)

func get(url string, v any) error {
	c := http.Client{Timeout: 2 * time.Second}
	r, err := c.Get(url)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(v)
}

// submitted records, for each request a client sent only after it saw its previous request
// commit, that previous request (both as client/id keys).
type submitted struct {
	after map[string]string
}

// canon re-encodes a JSON op with sorted keys, so byte-level formatting does not matter.
func canon(raw []byte) (string, string) {
	var m map[string]any
	if json.Unmarshal(raw, &m) != nil {
		return "", string(raw)
	}
	b, _ := json.Marshal(m)
	c, _ := m["client"].(string)
	id, _ := m["id"].(string)
	if id == "" {
		return "", string(b)
	}
	return c + "/" + id, string(b)
}

func loadSubmitted(path string) (*submitted, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	s := &submitted{after: map[string]string{}}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Bytes()
		var w struct { // bench writes {"op":…,"after":"<key it saw commit first>"}; scripts write the bare op
			Op    json.RawMessage `json:"op"`
			After string          `json:"after"`
		}
		if json.Unmarshal(line, &w) == nil && len(w.Op) > 0 && w.Op[0] == '{' {
			line = w.Op
		}
		k, _ := canon(line)
		if _, seen := s.after[k]; k == "" || seen {
			continue // a resend of the same request
		}
		if w.After != "" {
			s.after[k] = w.After
		}
	}
	return s, sc.Err()
}

// properties checks one honest ledger:
//   - validity: every applied op carries a valid signature of the client it names, so it is
//     exactly what that client sent;
//   - no duplicates: each request is applied once (later copies are marked Dup);
//   - real-time order: a request sent after the client saw an earlier one commit takes
//     effect after it, the ordering linearizability requires.
func properties(id string, lg []server.Block, sub *submitted) (bad bool) {
	applied := map[string]bool{}
	report := func(kind string, i uint64, format string, a ...any) {
		bad = true
		fmt.Printf("%s on %s at index %d: %s\n", kind, id, i, fmt.Sprintf(format, a...))
	}
	for _, b := range lg {
		for k, raw := range b.Ops {
			key, _ := canon(raw)
			dup := k < len(b.Dup) && b.Dup[k]
			switch {
			case key == "":
				report("VALIDITY VIOLATION", b.Index, "op without a request id")
				continue
			case dup && !applied[key]:
				report("DUPLICATE VIOLATION", b.Index, "%s marked duplicate but never applied", short(key))
			case !dup && applied[key]:
				report("DUPLICATE VIOLATION", b.Index, "%s applied twice", short(key))
			}
			if dup {
				continue
			}
			if !common.VerifyRequest(raw) {
				report("VALIDITY VIOLATION", b.Index, "%s is not what its client signed", short(key))
			}
			applied[key] = true
			if sub == nil {
				continue
			}
			if prev := sub.after[key]; prev != "" && !applied[prev] {
				report("REAL-TIME ORDER VIOLATION", b.Index, "%s took effect before %s, which committed before it was sent", short(key), short(prev))
			}
		}
	}
	return bad
}

// short abbreviates a request key: a client id is a 64-character public key.
func short(key string) string {
	if i := strings.Index(key, "/"); i > 8 {
		return key[:8] + "…" + key[i:]
	}
	return key
}

// logMatching: two honest ledgers that hold the same block hash at an index hold the
// same blocks at every index before it (Raft's Log Matching property, for the ledger).
func logMatching(ledgers map[string][]server.Block) (bad bool) {
	var ids []string
	for id := range ledgers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for a := 0; a < len(ids); a++ {
		for b := a + 1; b < len(ids); b++ {
			la, lb := ledgers[ids[a]], ledgers[ids[b]]
			for i := min(len(la), len(lb)) - 1; i >= 0; i-- {
				if la[i].Hash != lb[i].Hash {
					continue
				}
				for j := 0; j < i; j++ {
					if la[j].Hash != lb[j].Hash {
						bad = true
						fmt.Printf("LOG MATCHING VIOLATION: %s and %s share block %d but differ at %d\n", ids[a], ids[b], la[i].Index, la[j].Index)
						break
					}
				}
				break
			}
		}
	}
	return bad
}

func check(urls []string, sub *submitted) bool {
	seen := map[uint64]map[string][]string{}  // index -> Merkle root of the block's ops -> replicas
	heads := map[uint64]map[string][]string{} // index -> ledger hash -> replicas
	bad, checked := false, 0
	ledBy := map[uint64][]string{} // term -> honest replicas that became leader in it
	ledgers := map[string][]server.Block{}
	for _, u := range urls {
		var st struct {
			ID, Fault string
			Led       []uint64
		}
		if get(u+"/admin/status", &st) != nil {
			fmt.Println("unreachable:", u)
			continue
		}
		if st.Fault == "equivocate" {
			fmt.Println("skipping Byzantine replica", st.ID)
			continue
		}
		for _, t := range st.Led {
			ledBy[t] = append(ledBy[t], st.ID)
		}
		var lg []server.Block
		if get(u+"/admin/log", &lg) != nil {
			continue
		}
		checked++
		ledgers[st.ID] = lg
		if properties(st.ID, lg, sub) {
			bad = true
		}
		if i := server.VerifyChain(lg); i != 0 {
			bad = true
			fmt.Printf("LEDGER CORRUPT on %s: hash chain breaks at index %d\n", st.ID, i)
		}
		for _, e := range lg {
			add(seen, e.Index, e.Merkle[:8], st.ID)
			add(heads, e.Index, e.Hash[:8], st.ID)
		}
	}
	idxs := []int{}
	for i := range seen {
		idxs = append(idxs, int(i))
	}
	sort.Ints(idxs)
	for _, i := range idxs {
		if m := seen[uint64(i)]; len(m) > 1 {
			bad = true
			fmt.Printf("SAFETY VIOLATION at index %d: honest replicas disagree: %v\n", i, m)
		}
	}
	for _, i := range idxs { // the ledger forks at the first disagreement and stays forked
		if m := heads[uint64(i)]; len(m) > 1 {
			fmt.Printf("LEDGER FORK from height %d: %v\n", i, m)
			break
		}
	}
	for t, ids := range ledBy { // Raft only: at most one leader per term
		if len(ids) > 1 {
			bad = true
			fmt.Printf("ELECTION SAFETY VIOLATION: %v were all leader in term %d\n", ids, t)
		}
	}
	if logMatching(ledgers) {
		bad = true
	}
	if checked == 0 { // nothing to compare is not evidence of safety
		fmt.Println("NO HONEST REPLICA REACHABLE: nothing checked")
		return true
	}
	if !bad {
		fmt.Printf("SAFETY OK: %d honest replicas agree on all %d committed indices\n", checked, len(idxs))
		also := "no duplicates, log matching"
		if sub != nil {
			also = "validity, no duplicates, real-time order, log matching"
		}
		fmt.Printf("PROPERTIES OK: %s\n", also)
	}
	return bad
}

// evidence fetches each replica's equivocation evidence and re-checks every signature
// itself, so a replica cannot frame another: only the accused's own key could sign both messages.
func evidence(urls []string, verify *common.Auth) {
	holders := map[string][]string{} // verified proof, per accused/view/seq -> replicas holding it
	var keys []string
	for _, u := range urls {
		var st struct{ ID string }
		var ev []common.Evidence
		if get(u+"/admin/status", &st) != nil || get(u+"/admin/evidence", &ev) != nil {
			continue
		}
		for _, e := range ev {
			if !e.Check(verify) {
				fmt.Printf("INVALID PROOF from %s against %s (ignored)\n", st.ID, e.Accused)
				continue
			}
			k := fmt.Sprintf("%s signed two different pre-prepares for view %d seq %d", e.Accused, e.A.View, e.A.Seq)
			if holders[k] == nil {
				keys = append(keys, k)
			}
			holders[k] = append(holders[k], st.ID)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("EQUIVOCATION PROOF VERIFIED: %s (held by %s)\n", k, strings.Join(holders[k], ", "))
	}
}

func add(m map[uint64]map[string][]string, i uint64, k, id string) {
	if m[i] == nil {
		m[i] = map[string][]string{}
	}
	m[i][k] = append(m[i][k], id)
}

func main() {
	reps := flag.String("replicas", "", "comma-separated base URLs")
	once := flag.Bool("once", false, "check once; exit 1 on violation")
	keys := flag.String("keys", "keys", "directory holding pub.json")
	subPath := flag.String("submitted", "logs/submitted.jsonl", "ops clients sent, one JSON per line (real-time order)")
	flag.Parse()
	sub, err := loadSubmitted(*subPath)
	if err != nil {
		fmt.Println("no record of submitted ops, skipping the real-time order check:", err)
		sub = nil
	}
	urls := strings.Split(*reps, ",")
	pubs, err := common.LoadPublic(*keys)
	if err != nil {
		fmt.Println("no public keys, skipping evidence check:", err)
	}
	verify := common.NewAuth("checker", nil, pubs)
	for {
		bad := check(urls, sub)
		if pubs != nil {
			evidence(urls, verify)
		}
		if *once {
			if bad {
				os.Exit(1)
			}
			return
		}
		time.Sleep(time.Second)
	}
}
