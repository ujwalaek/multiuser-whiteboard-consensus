package server

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
)

// Block is one committed operation in the hash-chained ledger.
// Hash = sha256(prev || index || op), so two replicas that ever commit
// different ops at the same index hold different hashes from then on (a fork).
type Block struct {
	Index uint64          `json:"index"`
	Op    json.RawMessage `json:"op"`
	Prev  string          `json:"prev"`
	Hash  string          `json:"hash"`
	Dup   bool            `json:"dup,omitempty"` // request ID already applied; kept for the chain, not drawn
}

var genesis = hex.EncodeToString(make([]byte, sha256.Size))

func chain(prev string, index uint64, op []byte) string {
	p, _ := hex.DecodeString(prev)
	var ib [8]byte
	binary.BigEndian.PutUint64(ib[:], index)
	h := sha256.New()
	h.Write(p)
	h.Write(ib[:])
	h.Write(op)
	return hex.EncodeToString(h.Sum(nil))
}

// VerifyChain reports the first index whose hash does not follow from its predecessor, or 0 if the chain is intact.
func VerifyChain(bs []Block) uint64 {
	prev := genesis
	for _, b := range bs {
		if b.Prev != prev || b.Hash != chain(prev, b.Index, b.Op) {
			return b.Index
		}
		prev = b.Hash
	}
	return 0
}
