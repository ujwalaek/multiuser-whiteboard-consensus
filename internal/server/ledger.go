package server

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
)

// Block is one committed consensus entry in the hash-chained ledger: a batch of
// whiteboard operations under a Merkle root. Hash = sha256(prev || index || merkle),
// so two replicas that ever commit different ops at the same index hold different
// hashes from then on (a fork).
type Block struct {
	Index  uint64            `json:"index"`
	Ops    []json.RawMessage `json:"ops"`
	Merkle string            `json:"merkle"`
	Prev   string            `json:"prev"`
	Hash   string            `json:"hash"`
	Dup    []bool            `json:"dup,omitempty"` // per op: request ID already applied; chained, not drawn
}

var genesis = hex.EncodeToString(make([]byte, sha256.Size))

func raw(ops []json.RawMessage) [][]byte {
	b := make([][]byte, len(ops))
	for i, op := range ops {
		b[i] = op
	}
	return b
}

func chain(prev string, index uint64, merkle []byte) string {
	p, _ := hex.DecodeString(prev)
	var ib [8]byte
	binary.BigEndian.PutUint64(ib[:], index)
	h := sha256.New()
	h.Write(p)
	h.Write(ib[:])
	h.Write(merkle)
	return hex.EncodeToString(h.Sum(nil))
}

// seal fills in a block's Merkle root and hash from its ops and predecessor.
func seal(b *Block, prev string) {
	m := MerkleRoot(raw(b.Ops))
	b.Merkle, b.Prev, b.Hash = hex.EncodeToString(m), prev, chain(prev, b.Index, m)
}

// VerifyChain reports the first index whose Merkle root or hash does not follow from
// its ops and predecessor, or 0 if the chain is intact.
func VerifyChain(bs []Block) uint64 {
	prev := genesis
	for _, b := range bs {
		m := MerkleRoot(raw(b.Ops))
		if b.Prev != prev || b.Merkle != hex.EncodeToString(m) || b.Hash != chain(prev, b.Index, m) {
			return b.Index
		}
		prev = b.Hash
	}
	return 0
}

// decodeBatch splits a committed consensus entry into ops. The batcher proposes JSON
// arrays; a bare object (a single op) is accepted too. Anything else was still
// committed, so it is kept, as a JSON string, rather than dropped: skipping it would
// hide exactly the divergence the ledger exists to expose.
func decodeBatch(entry []byte) []json.RawMessage {
	var ops []json.RawMessage
	if json.Unmarshal(entry, &ops) == nil {
		return ops
	}
	if json.Valid(entry) {
		return []json.RawMessage{entry}
	}
	s, _ := json.Marshal(string(entry))
	return []json.RawMessage{s}
}
