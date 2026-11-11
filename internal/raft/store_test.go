package raft

import (
	"os"
	"path/filepath"
	"testing"

	"wb/pb"
)

// A restarted node gets back exactly the term, vote and log it had on disk, including
// after a conflict truncated the log; a record torn by a crash mid-write is dropped.
func TestStoreSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	n := &Node{log: []*pb.LogEntry{{}}}
	if err := n.Persist(dir); err != nil {
		t.Fatal(err)
	}
	n.term, n.votedFor = 3, "r2"
	n.saveMeta()
	for i := 1; i <= 4; i++ {
		e := &pb.LogEntry{Term: uint64(i), Op: op(i)}
		n.log = append(n.log, e)
		n.saveLog([]*pb.LogEntry{e}, false)
	}
	n.log = append(n.log[:3], &pb.LogEntry{Term: 3, Op: op(9)}) // conflict at index 3
	n.saveLog(nil, true)

	f, _ := os.OpenFile(filepath.Join(dir, "log.bin"), os.O_WRONLY|os.O_APPEND, 0)
	f.Write([]byte{0x40, 1, 2}) // a record that says 64 bytes but stops after 2
	f.Close()

	r := &Node{log: []*pb.LogEntry{{}}}
	if err := r.Persist(dir); err != nil {
		t.Fatal(err)
	}
	if r.term != 3 || r.votedFor != "r2" {
		t.Errorf("restored term %d vote %q, want 3 r2", r.term, r.votedFor)
	}
	if len(r.log) != 4 || string(r.log[3].Op) != string(op(9)) || r.log[2].Term != 2 {
		t.Errorf("restored log has %d entries (last %s), want 3 ending in op 9", len(r.log)-1, r.log[len(r.log)-1].Op)
	}
}
