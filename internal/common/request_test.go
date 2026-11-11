package common

import (
	"testing"

	"wb/internal/fault"
)

func TestSignedRequests(t *testing.T) {
	priv, id := NewClientKey()
	req := SignRequest(priv, []byte(`{"kind":"draw","id":"s1","client":"`+id+`","shape":{"color":"#0000ff"}}`))
	if !VerifyRequest(req) {
		t.Fatal("a correctly signed request does not verify")
	}
	if VerifyRequest(fault.Mutate(req)) {
		t.Error("a request altered by a Byzantine replica still verifies")
	}
	_, other := NewClientKey()
	forged := SignRequest(priv, []byte(`{"kind":"draw","id":"s1","client":"`+other+`"}`))
	if VerifyRequest(forged) {
		t.Error("a request signed with someone else's key verifies")
	}
	if VerifyRequest([]byte(`{"kind":"draw","id":"s1","client":"x"}`)) {
		t.Error("an unsigned request verifies")
	}
	if !ValidBatch([]byte("["+string(req)+","+string(req)+"]")) || !ValidBatch([]byte("[]")) {
		t.Error("a batch of signed requests, or the empty batch, is rejected")
	}
	if ValidBatch([]byte("[" + string(req) + `,{"id":"x"}]`)) {
		t.Error("a batch with one unsigned request is accepted")
	}
}
