// sign reads a whiteboard op (JSON) on stdin and prints it as a signed client request:
// "client" set to this client's id (its public key) and "sig" appended. The key is kept in
// -key, created on first use, so a script signs all its ops as one client.
package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"wb/internal/common"
)

func main() {
	path := flag.String("key", "keys/client.key", "client key file (hex seed)")
	flag.Parse()
	var priv ed25519.PrivateKey
	if b, err := os.ReadFile(*path); err == nil {
		seed, err := hex.DecodeString(string(b))
		if err != nil || len(seed) != ed25519.SeedSize {
			log.Fatalf("bad key file %s", *path)
		}
		priv = ed25519.NewKeyFromSeed(seed)
	} else {
		priv, _ = common.NewClientKey()
		if err := os.WriteFile(*path, []byte(hex.EncodeToString(priv.Seed())), 0o600); err != nil {
			log.Fatal(err)
		}
	}
	in, _ := io.ReadAll(os.Stdin)
	var op map[string]any
	if err := json.Unmarshal(in, &op); err != nil {
		log.Fatal(err)
	}
	op["client"] = hex.EncodeToString(priv.Public().(ed25519.PublicKey))
	delete(op, "sig")
	b, _ := json.Marshal(op)
	fmt.Println(string(common.SignRequest(priv, b)))
}
