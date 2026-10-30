package common

import (
	"crypto/hmac"
	"crypto/sha256"
	"testing"

	"wb/pb"
)

// E6: the per-message cost of Ed25519 against the per-pair HMAC it replaced.
// Run with: go test ./internal/common -bench . -benchmem

func benchMsg() *pb.PhaseMsg {
	return &pb.PhaseMsg{View: 0, Seq: 42, Digest: make([]byte, 32), Sender: "r1"}
}

func BenchmarkEd25519Sign(b *testing.B) {
	privs, pubs := GenerateKeys(2)
	a, m := NewAuth("r1", privs["r1"], pubs), benchMsg()
	for i := 0; i < b.N; i++ {
		a.Sign(m)
	}
}

func BenchmarkEd25519Verify(b *testing.B) {
	privs, pubs := GenerateKeys(2)
	a, m := NewAuth("r1", privs["r1"], pubs), benchMsg()
	a.Sign(m)
	for i := 0; i < b.N; i++ {
		if !a.Verify("r1", m) {
			b.Fatal("bad signature")
		}
	}
}

// BenchmarkHMAC does the same work as the old auth: encode the message, then HMAC-SHA256.
func BenchmarkHMAC(b *testing.B) {
	key, m := make([]byte, 32), benchMsg()
	for i := 0; i < b.N; i++ {
		h := hmac.New(sha256.New, key)
		h.Write(signedBytes(m))
		h.Sum(nil)
	}
}
