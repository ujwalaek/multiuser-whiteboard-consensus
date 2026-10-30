package common

import (
	"testing"

	"wb/pb"
)

func TestSignAndVerify(t *testing.T) {
	privs, pubs := GenerateKeys(3)
	r0, r1 := NewAuth("r0", privs["r0"], pubs), NewAuth("r1", privs["r1"], pubs)
	m := &pb.PhaseMsg{View: 0, Seq: 7, Digest: []byte("d"), Sender: "r0"}
	r0.Sign(m)

	if !r1.Verify("r0", m) {
		t.Fatal("valid signature rejected")
	}
	if r1.Verify("r2", m) {
		t.Fatal("signature accepted for the wrong sender")
	}
	m.Seq = 8
	if r1.Verify("r0", m) {
		t.Fatal("tampered message accepted")
	}
}

// r1 knows only r0's public key, so it cannot produce a message that verifies as r0's.
func TestCannotForgeAnotherReplica(t *testing.T) {
	privs, pubs := GenerateKeys(2)
	r1 := NewAuth("r1", privs["r1"], pubs)
	m := &pb.PrePrepareMsg{Seq: 1, Digest: []byte("fake"), Sender: "r0"}
	r1.Sign(m) // r1 claims to be r0
	if NewAuth("x", nil, pubs).Verify("r0", m) {
		t.Fatal("r1 forged a message from r0")
	}
}

func TestKeysRoundTripThroughFiles(t *testing.T) {
	dir := t.TempDir()
	privs, pubs := GenerateKeys(2)
	if err := WriteKeys(dir, privs, pubs); err != nil {
		t.Fatal(err)
	}
	a, err := LoadAuth(dir, "r1")
	if err != nil {
		t.Fatal(err)
	}
	m := &pb.VoteReq{Term: 3, Candidate: "r1", Sender: "r1"}
	a.Sign(m)
	if !NewAuth("x", nil, pubs).Verify("r1", m) {
		t.Fatal("key loaded from file does not match the generated one")
	}
}

func TestEvidenceCheck(t *testing.T) {
	privs, pubs := GenerateKeys(2)
	r0 := NewAuth("r0", privs["r0"], pubs)
	a := &pb.PrePrepareMsg{Seq: 1, Digest: []byte("A"), Sender: "r0"}
	b := &pb.PrePrepareMsg{Seq: 1, Digest: []byte("B"), Sender: "r0"}
	r0.Sign(a)
	r0.Sign(b)
	v := NewAuth("x", nil, pubs)
	if !(Evidence{Accused: "r0", A: a, B: b}).Check(v) {
		t.Fatal("real equivocation not proven")
	}
	if (Evidence{Accused: "r0", A: a, B: a}).Check(v) {
		t.Fatal("two identical messages accepted as proof")
	}
	if (Evidence{Accused: "r1", A: a, B: b}).Check(v) {
		t.Fatal("evidence pinned on the wrong replica")
	}
}
