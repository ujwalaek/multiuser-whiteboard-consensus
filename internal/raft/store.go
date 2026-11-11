package raft

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/proto"

	"wb/pb"
)

// store keeps what Raft must not forget across a restart: the current term, the vote cast
// in it, and the log. Every change is on disk (fsync) before the node answers an RPC or
// counts its own vote, so a replica that crashes and restarts cannot vote twice in a term
// or deny an entry it acknowledged. The commit index is not stored: it is relearned from
// the leader, and committed entries are applied again from the start of the log.
type store struct {
	meta, logPath string
	logf          *os.File
}

type meta struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"voted_for"`
}

// openStore opens (or creates) dir and returns what a previous run left there.
func openStore(dir string) (*store, meta, []*pb.LogEntry, error) {
	var m meta
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, m, nil, err
	}
	s := &store{meta: filepath.Join(dir, "meta.json"), logPath: filepath.Join(dir, "log.bin")}
	if b, err := os.ReadFile(s.meta); err == nil {
		if err := json.Unmarshal(b, &m); err != nil {
			return nil, m, nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, m, nil, err
	}
	entries, err := readLog(s.logPath)
	if err != nil {
		return nil, m, nil, err
	}
	if s.logf, err = os.OpenFile(s.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err != nil {
		return nil, m, nil, err
	}
	return s, m, entries, nil
}

// readLog reads length-prefixed entries; a torn last record (crash mid-write) is dropped.
func readLog(path string) ([]*pb.LogEntry, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var out []*pb.LogEntry
	for {
		size, err := binary.ReadUvarint(r)
		if err != nil {
			return out, nil
		}
		b := make([]byte, size)
		if _, err := io.ReadFull(r, b); err != nil {
			return out, nil
		}
		e := &pb.LogEntry{}
		if proto.Unmarshal(b, e) != nil {
			return out, nil
		}
		out = append(out, e)
	}
}

func (s *store) saveMeta(m meta) error {
	b, _ := json.Marshal(m)
	tmp := s.meta + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	return os.Rename(tmp, s.meta) // atomic: a crash leaves the old or the new term, never half of one
}

func record(e *pb.LogEntry) []byte {
	b, _ := proto.Marshal(e)
	return append(binary.AppendUvarint(nil, uint64(len(b))), b...)
}

// append adds entries to the end of the log file.
func (s *store) append(entries []*pb.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	var buf []byte
	for _, e := range entries {
		buf = append(buf, record(e)...)
	}
	if _, err := s.logf.Write(buf); err != nil {
		return err
	}
	return s.logf.Sync()
}

// rewrite replaces the log file with entries (after a conflict truncated the log).
func (s *store) rewrite(entries []*pb.LogEntry) error {
	tmp := s.logPath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if _, err := f.Write(record(e)); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	if err := os.Rename(tmp, s.logPath); err != nil {
		return err
	}
	s.logf.Close()
	s.logf, err = os.OpenFile(s.logPath, os.O_WRONLY|os.O_APPEND, 0o644)
	return err
}
