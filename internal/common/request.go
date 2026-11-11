package common

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
)

// Client requests are signed, so a Byzantine primary cannot alter what a client asked
// for. A client's id is its Ed25519 public key in hex, which makes the id self-certifying:
// no key registry is needed, and a request claiming to come from a client verifies only
// under that client's own key.
//
// Wire form: the client writes its op as compact JSON (any field order, "client" set to
// its id), signs exactly those bytes, and appends the signature as a last field:
//
//	{"kind":"draw","id":"s1","client":"<64 hex>",…,"sig":"<128 hex>"}
//
// The signed bytes are the request with `,"sig":"…"` removed.

var reqSig = []byte(`,"sig":"`)

// SignRequest returns op (compact JSON object, without sig) with priv's signature appended.
func SignRequest(priv ed25519.PrivateKey, op []byte) []byte {
	sig := hex.EncodeToString(ed25519.Sign(priv, op))
	out := append([]byte{}, op[:len(op)-1]...)
	return append(append(append(out, reqSig...), sig...), '"', '}')
}

// VerifyRequest reports whether raw carries a valid signature by the client it names.
func VerifyRequest(raw []byte) bool {
	raw = bytes.TrimSpace(raw)
	i := bytes.LastIndex(raw, reqSig)
	if i < 0 || len(raw) < i+len(reqSig)+2 || !bytes.HasSuffix(raw, []byte(`"}`)) {
		return false
	}
	sig, err := hex.DecodeString(string(raw[i+len(reqSig) : len(raw)-2]))
	if err != nil || len(sig) != ed25519.SignatureSize {
		return false
	}
	signed := append(append([]byte{}, raw[:i]...), '}')
	var req struct{ Client string }
	if json.Unmarshal(signed, &req) != nil {
		return false
	}
	pub, err := hex.DecodeString(req.Client)
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return false
	}
	return ed25519.Verify(pub, signed, sig)
}

// ValidBatch reports whether every op in a consensus entry (a JSON array of requests) is
// validly signed. The empty batch, which fills a slot after a view change, is valid.
func ValidBatch(entry []byte) bool {
	var ops []json.RawMessage
	if json.Unmarshal(entry, &ops) != nil {
		return VerifyRequest(entry) // an unbatched single request
	}
	for _, op := range ops {
		if !VerifyRequest(op) {
			return false
		}
	}
	return true
}

// NewClientKey returns a fresh client key and the id that goes with it.
func NewClientKey() (ed25519.PrivateKey, string) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		panic(err)
	}
	return priv, hex.EncodeToString(pub)
}
