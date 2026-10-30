package common

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var ErrNotLeader = errors.New("not leader")

// BasePort is replica i's gRPC port minus i (tests move it to avoid clashing with a running cluster).
var BasePort = 7000

type CommitFn func(index uint64, op []byte)

type Consensus interface {
	// Propose returns a leader hint (e.g. "r2") when ErrNotLeader is returned.
	Propose(op []byte) (hint string, err error)
	Status() map[string]any
}

// Num("r3") == 3
func Num(id string) int { n, _ := strconv.Atoi(id[1:]); return n }

func Dial(self string, n int) map[string]*grpc.ClientConn {
	m := map[string]*grpc.ClientConn{}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("r%d", i)
		if id == self {
			continue
		}
		c, err := grpc.NewClient(fmt.Sprintf("localhost:%d", BasePort+i),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatal(err)
		}
		m[id] = c
	}
	return m
}

func Ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 300*time.Millisecond)
}