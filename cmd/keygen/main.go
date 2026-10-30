// keygen writes an Ed25519 key pair for each replica: keys/r<i>.key (private,
// one per replica) and keys/pub.json (every public key, shared by all).
package main

import (
	"flag"
	"log"

	"wb/internal/common"
)

func main() {
	n := flag.Int("n", 4, "number of replicas")
	dir := flag.String("dir", "keys", "output directory")
	flag.Parse()
	privs, pubs := common.GenerateKeys(*n)
	if err := common.WriteKeys(*dir, privs, pubs); err != nil {
		log.Fatal(err)
	}
	log.Printf("wrote %d key pairs to %s/", *n, *dir)
}
