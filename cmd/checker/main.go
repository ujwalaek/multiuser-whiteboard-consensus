package main

import (
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

func check(urls []string) bool {
	seen := map[uint64]map[string][]string{} // index -> Merkle root of the block's ops -> replicas
	heads := map[uint64]map[string][]string{} // index -> ledger hash -> replicas
	bad, checked := false, 0
	ledBy := map[uint64][]string{} // term -> honest replicas that became leader in it
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
	if checked == 0 { // nothing to compare is not evidence of safety
		fmt.Println("NO HONEST REPLICA REACHABLE: nothing checked")
		return true
	}
	if !bad {
		fmt.Printf("SAFETY OK: %d honest replicas agree on all %d committed indices\n", checked, len(idxs))
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
	flag.Parse()
	urls := strings.Split(*reps, ",")
	pubs, err := common.LoadPublic(*keys)
	if err != nil {
		fmt.Println("no public keys, skipping evidence check:", err)
	}
	verify := common.NewAuth("checker", nil, pubs)
	for {
		bad := check(urls)
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