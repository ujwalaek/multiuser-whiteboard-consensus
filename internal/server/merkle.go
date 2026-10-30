package server

import "crypto/sha256"

// Merkle trees over a block's operations. Leaves and inner nodes are hashed with
// different prefixes so a leaf can never be passed off as an inner node, and an odd
// node is promoted unchanged rather than paired with a copy of itself (duplicating it
// would let two different op lists share a root).

func leafHash(op []byte) []byte {
	h := sha256.Sum256(append([]byte{0}, op...))
	return h[:]
}

func nodeHash(l, r []byte) []byte {
	h := sha256.New()
	h.Write([]byte{1})
	h.Write(l)
	h.Write(r)
	return h.Sum(nil)
}

func level(hs [][]byte) [][]byte {
	var up [][]byte
	for i := 0; i < len(hs); i += 2 {
		if i+1 == len(hs) {
			up = append(up, hs[i])
		} else {
			up = append(up, nodeHash(hs[i], hs[i+1]))
		}
	}
	return up
}

// MerkleRoot returns the root over ops (sha256 of nothing for an empty list).
func MerkleRoot(ops [][]byte) []byte {
	if len(ops) == 0 {
		h := sha256.Sum256(nil)
		return h[:]
	}
	hs := make([][]byte, len(ops))
	for i, op := range ops {
		hs[i] = leafHash(op)
	}
	for len(hs) > 1 {
		hs = level(hs)
	}
	return hs[0]
}

// ProofStep is one sibling on the path from a leaf to the root.
type ProofStep struct {
	Hash []byte `json:"hash"`
	Left bool   `json:"left"` // sibling sits to the left of the running hash
}

// MerkleProof returns the sibling path that shows ops[i] is under MerkleRoot(ops).
func MerkleProof(ops [][]byte, i int) []ProofStep {
	hs := make([][]byte, len(ops))
	for k, op := range ops {
		hs[k] = leafHash(op)
	}
	var path []ProofStep
	for len(hs) > 1 {
		if sib := i ^ 1; sib < len(hs) {
			path = append(path, ProofStep{hs[sib], sib < i})
		}
		hs, i = level(hs), i/2
	}
	return path
}

// VerifyProof reports whether op, followed up path, reaches root.
func VerifyProof(op []byte, path []ProofStep, root []byte) bool {
	h := leafHash(op)
	for _, s := range path {
		if s.Left {
			h = nodeHash(s.Hash, h)
		} else {
			h = nodeHash(h, s.Hash)
		}
	}
	return string(h) == string(root)
}
