package common

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Auth signs this replica's messages with its Ed25519 private key and verifies
// peers' messages against their public keys. Unlike a shared HMAC key, a signature
// can only come from its owner, so two conflicting signed messages from the same
// replica are proof, to anyone, that it equivocated.
type Auth struct {
	self string
	priv ed25519.PrivateKey
	pubs map[string]ed25519.PublicKey
}

func NewAuth(self string, priv ed25519.PrivateKey, pubs map[string]ed25519.PublicKey) *Auth {
	return &Auth{self, priv, pubs}
}

// GenerateKeys makes a key pair for each of replicas r0..r(n-1).
func GenerateKeys(n int) (map[string]ed25519.PrivateKey, map[string]ed25519.PublicKey) {
	privs, pubs := map[string]ed25519.PrivateKey{}, map[string]ed25519.PublicKey{}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("r%d", i)
		pub, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			panic(err)
		}
		privs[id], pubs[id] = priv, pub
	}
	return privs, pubs
}

// WriteKeys stores each private key as dir/<id>.key and all public keys in dir/pub.json.
func WriteKeys(dir string, privs map[string]ed25519.PrivateKey, pubs map[string]ed25519.PublicKey) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	for id, k := range privs {
		if err := os.WriteFile(filepath.Join(dir, id+".key"), []byte(hex.EncodeToString(k.Seed())), 0o600); err != nil {
			return err
		}
	}
	hp := map[string]string{}
	for id, k := range pubs {
		hp[id] = hex.EncodeToString(k)
	}
	b, _ := json.MarshalIndent(hp, "", "  ")
	return os.WriteFile(filepath.Join(dir, "pub.json"), b, 0o644)
}

// LoadAuth reads this replica's private key and every replica's public key from dir.
func LoadAuth(dir, self string) (*Auth, error) {
	seed, err := os.ReadFile(filepath.Join(dir, self+".key"))
	if err != nil {
		return nil, err
	}
	s, err := hex.DecodeString(string(seed))
	if err != nil || len(s) != ed25519.SeedSize {
		return nil, fmt.Errorf("bad key file for %s", self)
	}
	pubs, err := LoadPublic(dir)
	if err != nil {
		return nil, err
	}
	return NewAuth(self, ed25519.NewKeyFromSeed(s), pubs), nil
}

// LoadPublic reads every replica's public key from dir/pub.json. An Auth built from
// these alone can verify but not sign, which is all an outside auditor needs.
func LoadPublic(dir string) (map[string]ed25519.PublicKey, error) {
	b, err := os.ReadFile(filepath.Join(dir, "pub.json"))
	if err != nil {
		return nil, err
	}
	var hp map[string]string
	if err := json.Unmarshal(b, &hp); err != nil {
		return nil, err
	}
	pubs := map[string]ed25519.PublicKey{}
	for id, h := range hp {
		k, err := hex.DecodeString(h)
		if err != nil || len(k) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("bad public key for %s", id)
		}
		pubs[id] = k
	}
	return pubs, nil
}

func sigField(m proto.Message) protoreflect.FieldDescriptor {
	return m.ProtoReflect().Descriptor().Fields().ByName("sig")
}

// signedBytes is the message encoded deterministically with its own sig cleared.
func signedBytes(m proto.Message) []byte {
	c := proto.Clone(m).ProtoReflect()
	c.Clear(sigField(m))
	b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c.Interface())
	return b
}

// Sign sets m's sig field to this replica's signature over the rest of m.
func (a *Auth) Sign(m proto.Message) {
	m.ProtoReflect().Set(sigField(m), protoreflect.ValueOfBytes(ed25519.Sign(a.priv, signedBytes(m))))
}

// Verify reports whether m carries a valid signature by sender.
func (a *Auth) Verify(sender string, m proto.Message) bool {
	pub, ok := a.pubs[sender]
	if !ok {
		return false
	}
	return ed25519.Verify(pub, signedBytes(m), m.ProtoReflect().Get(sigField(m)).Bytes())
}
