// bench drives a running cluster with closed-loop drawing clients and reports commit
// latency, throughput and message cost as one JSON line (appended to -out).
//
// Each client submits a stroke, waits until it shows up in a committed block on the
// watched replica's WebSocket, then submits the next. Latency is submit -> commit.
// With -crash-after, the leader is crashed mid-run and max_gap_ms is the longest
// stretch with no commits: how long the whiteboard froze.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"wb/internal/common"
)

type status struct {
	ID, Role, Fault, Proto string
	AppliedOps             int64 `json:"applied_ops"`
}

func init() { log.SetFlags(0) }

var httpc = http.Client{Timeout: 2 * time.Second}

func url(i int) string { return fmt.Sprintf("http://localhost:%d", 8000+i) }

func getStatus(i int) (st status, err error) {
	r, err := httpc.Get(url(i) + "/admin/status")
	if err != nil {
		return st, err
	}
	defer r.Body.Close()
	err = json.NewDecoder(r.Body).Decode(&st)
	return st, err
}

// sentTotals sums messages and bytes sent by every reachable replica.
func sentTotals(n int) (msgs, bytes int64) {
	for i := 0; i < n; i++ {
		r, err := httpc.Get(url(i) + "/admin/status")
		if err != nil {
			continue
		}
		var m struct {
			Sent      int64 `json:"sent"`
			SentBytes int64 `json:"sent_bytes"`
		}
		json.NewDecoder(r.Body).Decode(&m)
		r.Body.Close()
		msgs, bytes = msgs+m.Sent, bytes+m.SentBytes
	}
	return
}

func leader(n int) int {
	for i := 0; i < n; i++ {
		if st, err := getStatus(i); err == nil && st.Fault != "crash" && (st.Role == "leader" || st.Role == "primary") {
			return i
		}
	}
	return -1
}

func main() {
	n := flag.Int("n", 4, "replicas, on ports 8000..")
	clients := flag.Int("clients", 8, "concurrent closed-loop clients")
	dur := flag.Duration("duration", 8*time.Second, "measured run length")
	warm := flag.Duration("warmup", 1*time.Second, "unmeasured time before the run")
	watch := flag.Int("watch", -1, "replica whose commits define latency (-1: the leader, -2: a follower)")
	crashAfter := flag.Duration("crash-after", 0, "crash the leader this long into the run (0: never)")
	label := flag.String("label", "", "free-form tag stored with the result")
	out := flag.String("out", "", "append the JSON result to this file")
	sentLog := flag.String("log", "logs/submitted.jsonl", "append every op sent here, for the checker (empty: off)")
	flag.Parse()

	var l int
	for end := time.Now().Add(10 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		if l = leader(*n); l >= 0 {
			break
		}
		if time.Now().After(end) {
			log.Fatal("no leader")
		}
	}
	w := *watch
	switch w {
	case -1:
		w = l
	case -2: // a follower keeps reporting commits after the leader is crashed
		w = (l + 1) % *n
	}
	st, _ := getStatus(w)

	// commit notifications from the watched replica
	var mu sync.Mutex
	waiting := map[string]chan struct{}{}
	var commitTimes []time.Time
	conn, _, err := websocket.DefaultDialer.Dial(fmt.Sprintf("ws://localhost:%d/ws", 8000+w), nil)
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		for {
			var b struct{ Ops []struct{ ID string } }
			if conn.ReadJSON(&b) != nil {
				return
			}
			now := time.Now()
			mu.Lock()
			commitTimes = append(commitTimes, now)
			for _, op := range b.Ops {
				if ch, ok := waiting[op.ID]; ok {
					close(ch)
					delete(waiting, op.ID)
				}
			}
			mu.Unlock()
		}
	}()

	var sentMu sync.Mutex
	var sentF *os.File
	if *sentLog != "" {
		if sentF, err = os.OpenFile(*sentLog, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); err != nil {
			log.Fatal(err)
		}
		defer sentF.Close()
	}
	record := func(body []byte, after string) { // after: the request this client saw commit just before
		if sentF != nil {
			line, _ := json.Marshal(map[string]any{"op": json.RawMessage(body), "after": after})
			sentMu.Lock()
			sentF.Write(append(line, '\n'))
			sentMu.Unlock()
		}
	}

	var target atomic.Int64 // replica currently believed to lead
	target.Store(int64(l))
	broadcast := func(body []byte) bool { // the primary did not take it: tell every replica
		ok := false
		for i := 0; i < *n; i++ {
			r, err := httpc.Post(url(i)+"/submit", "application/json", strings.NewReader(string(body)))
			if err != nil {
				continue
			}
			var res struct{ OK bool }
			json.NewDecoder(r.Body).Decode(&res)
			r.Body.Close()
			ok = ok || res.OK
		}
		return ok
	}
	submit := func(body []byte) bool {
		for try := 0; try < 20; try++ {
			if try == 2 && broadcast(body) {
				return true
			}
			r, err := httpc.Post(url(int(target.Load()))+"/submit", "application/json", strings.NewReader(string(body)))
			if err == nil {
				var res struct {
					OK       bool
					Redirect string
				}
				json.NewDecoder(r.Body).Decode(&res)
				r.Body.Close()
				if res.OK {
					return true
				}
			}
			time.Sleep(50 * time.Millisecond) // not the leader any more: find the new one
			if nl := leader(*n); nl >= 0 {
				target.Store(int64(nl))
			}
		}
		return false
	}

	start := time.Now()
	measureFrom, stop := start.Add(*warm), start.Add(*warm+*dur)
	var lat []float64
	var timeouts atomic.Int64
	var wg sync.WaitGroup
	var m0, b0 int64
	var ops0 int64
	go func() { // snapshot counters when measurement starts
		time.Sleep(*warm)
		m0, b0 = sentTotals(*n)
		s, _ := getStatus(w)
		ops0 = s.AppliedOps
	}()
	if *crashAfter > 0 {
		go func() {
			time.Sleep(*warm + *crashAfter)
			httpc.Post(fmt.Sprintf("%s/admin/fault?mode=crash", url(int(target.Load()))), "", nil)
		}()
	}
	for c := 0; c < *clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			prev := "" // last request of this client known to have committed
			key, cid := common.NewClientKey()
			for k := 0; time.Now().Before(stop); k++ {
				id := fmt.Sprintf("b%d-%d-%d", c, k, start.UnixNano())
				body, _ := json.Marshal(map[string]any{"kind": "draw", "id": id, "client": cid,
					"lamport": k, "shape": map[string]any{"type": "polyline", "color": "#0000ff",
						"points": [][2]int{{10 * c, k % 500}, {10*c + 40, k%500 + 30}, {10*c + 80, k % 500}}}})
				body = common.SignRequest(key, body)
				record(body, prev)
				prev = ""
				ch := make(chan struct{})
				mu.Lock()
				waiting[id] = ch
				mu.Unlock()
				t0 := time.Now()
				// Wait for the commit, re-sending the same request every 500 ms (its id makes a
				// second copy harmless): an op accepted by a leader that then crashed is lost, and
				// a real client retries rather than waiting out a long timeout.
				ok := submit(body)
				giveUp := time.After(3 * time.Second)
			wait:
				for {
					select {
					case <-ch:
						break wait
					case <-time.After(500 * time.Millisecond):
						go submit(body)
					case <-giveUp:
						ok = false
						break wait
					}
				}
				if !ok {
					timeouts.Add(1)
					mu.Lock()
					delete(waiting, id)
					mu.Unlock()
					continue
				}
				prev = cid + "/" + id
				if t0.After(measureFrom) && time.Now().Before(stop) {
					mu.Lock()
					lat = append(lat, float64(time.Since(t0).Microseconds())/1000)
					mu.Unlock()
				}
			}
		}(c)
	}
	wg.Wait()
	m1, b1 := sentTotals(*n)
	s1, _ := getStatus(w)
	ops := s1.AppliedOps - ops0

	sort.Float64s(lat)
	pct := func(p float64) float64 {
		if len(lat) == 0 {
			return math.NaN()
		}
		return lat[int(p*float64(len(lat)-1))]
	}
	mean := 0.0
	for _, x := range lat {
		mean += x
	}
	if len(lat) > 0 {
		mean /= float64(len(lat))
	}
	gap := 0.0 // longest stretch without a commit during the measured window
	mu.Lock()
	prev := measureFrom
	for _, t := range commitTimes {
		if t.Before(measureFrom) || t.After(stop) {
			continue
		}
		gap = math.Max(gap, float64(t.Sub(prev).Milliseconds()))
		prev = t
	}
	gap = math.Max(gap, float64(stop.Sub(prev).Milliseconds())) // a freeze that lasts to the end counts too
	mu.Unlock()
	perOp := func(x int64) float64 {
		if ops <= 0 {
			return math.NaN()
		}
		return float64(x) / float64(ops)
	}
	res := map[string]any{
		"label": *label, "proto": st.Proto, "n": *n, "clients": *clients,
		"ops": len(lat), "throughput": float64(len(lat)) / dur.Seconds(),
		"p50_ms": pct(0.5), "p90_ms": pct(0.9), "p99_ms": pct(0.99), "mean_ms": mean,
		"msgs_per_op": perOp(m1 - m0), "bytes_per_op": perOp(b1 - b0),
		"timeouts": timeouts.Load(), "max_gap_ms": gap,
	}
	for k, v := range res { // JSON has no NaN: a run that measured nothing reports null
		if f, ok := v.(float64); ok && math.IsNaN(f) {
			res[k] = nil
		}
	}
	line, _ := json.Marshal(res)
	fmt.Println(string(line))
	if *out != "" {
		f, err := os.OpenFile(*out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			log.Fatal(err)
		}
		f.Write(append(line, '\n'))
		f.Close()
	}
	conn.Close()
}
