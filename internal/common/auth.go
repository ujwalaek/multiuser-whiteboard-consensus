package common

import (
	"crypto/hmac"
	"crypto/sha256"
	"os"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type Auth struct {
	master []byte
	self   string
}

func NewAuth(self string) *Auth {
	s := os.Getenv("WB_SECRET")
	if s == "" {
		s = "dev-secret"
	}
	return &Auth{[]byte(s), self}
}

// per-pair key: same for (a,b) and (b,a)
func (a *Auth) key(peer string) []byte {
	x, y := a.self, peer
	if y < x {
		x, y = y, x
	}
	h := hmac.New(sha256.New, a.master)
	h.Write([]byte(x + "|" + y))
	return h.Sum(nil)
}

func macOf(key []byte, m proto.Message) []byte {
	c := proto.Clone(m).ProtoReflect()
	c.Clear(c.Descriptor().Fields().ByName("mac"))
	b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c.Interface())
	h := hmac.New(sha256.New, key)
	h.Write(b)
	return h.Sum(nil)
}

// Sign must be called once per destination (the MAC differs per peer).
func (a *Auth) Sign(peer string, m proto.Message) {
	r := m.ProtoReflect()
	r.Set(r.Descriptor().Fields().ByName("mac"), protoreflect.ValueOfBytes(macOf(a.key(peer), m)))
}

func (a *Auth) Verify(sender string, m proto.Message) bool {
	r := m.ProtoReflect()
	got := r.Get(r.Descriptor().Fields().ByName("mac")).Bytes()
	return hmac.Equal(got, macOf(a.key(sender), m))
}