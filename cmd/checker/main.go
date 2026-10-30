package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

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
	seen := map[uint64]map[string][]string{} // index -> opHash -> replicas
	heads := map[uint64]map[string][]string{} // index -> ledger hash -> replicas
	bad := false
	for _, u := range urls {
		var st struct{ ID, Fault string }
		if get(u+"/admin/status", &st) != nil {
			fmt.Println("unreachable:", u)
			continue
		}
		if st.Fault == "equivocate" {
			fmt.Println("skipping Byzantine replica", st.ID)
			continue
		}
		var lg []server.Block
		if get(u+"/admin/log", &lg) != nil {
			continue
		}
		if i := server.VerifyChain(lg); i != 0 {
			bad = true
			fmt.Printf("LEDGER CORRUPT on %s: hash chain breaks at index %d\n", st.ID, i)
		}
		for _, e := range lg {
			h := sha256.Sum256(e.Op)
			add(seen, e.Index, hex.EncodeToString(h[:4]), st.ID)
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
	if !bad {
		fmt.Printf("SAFETY OK: honest replicas agree on all %d committed indices\n", len(idxs))
	}
	return bad
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
	flag.Parse()
	urls := strings.Split(*reps, ",")
	for {
		bad := check(urls)
		if *once {
			if bad {
				os.Exit(1)
			}
			return
		}
		time.Sleep(time.Second)
	}
}