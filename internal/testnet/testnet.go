// Package testnet runs a cluster of replicas inside one test process, over real gRPC on localhost.
package testnet

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"wb/internal/common"
	"wb/internal/fault"
)

// Replica is what each test cluster member records: its fault injector and the ops it committed, in order.
type Replica struct {
	ID   string
	Inj  *fault.Injector
	Auth *common.Auth
	mu   sync.Mutex
	log  map[uint64][]byte
}

func (r *Replica) OnCommit(i uint64, op []byte) {
	r.mu.Lock()
	r.log[i] = op
	r.mu.Unlock()
}

func (r *Replica) Committed() map[uint64][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := map[uint64][]byte{}
	for k, v := range r.log {
		m[k] = v
	}
	return m
}

// Start creates n replicas with gRPC servers on basePort+i. register is called once per replica
// to build its consensus node and attach it to the server. Servers stop when the test ends.
func Start(t *testing.T, n, basePort int, register func(r *Replica, conns map[string]*grpc.ClientConn, gs *grpc.Server)) []*Replica {
	t.Helper()
	common.BasePort = basePort
	privs, pubs := common.GenerateKeys(n)
	rs := make([]*Replica, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("r%d", i)
		r := &Replica{ID: id, Inj: fault.New(), Auth: common.NewAuth(id, privs[id], pubs), log: map[uint64][]byte{}}
		inj := r.Inj
		gs := grpc.NewServer(grpc.UnaryInterceptor(
			func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
				if inj.Drop() {
					return nil, status.Error(codes.Unavailable, "fault: dropped")
				}
				return h(ctx, req)
			}))
		register(r, common.Dial(r.ID, n), gs)
		lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", basePort+i))
		if err != nil {
			t.Fatal(err)
		}
		go gs.Serve(lis)
		t.Cleanup(gs.Stop)
		rs[i] = r
	}
	return rs
}

// Eventually polls cond every 50ms until it holds or the timeout passes.
func Eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(timeout); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}

// Agree fails the test if any two replicas outside skip committed different ops at the same index.
func Agree(t *testing.T, rs []*Replica, skip ...string) {
	t.Helper()
	at := map[uint64]string{}
	who := map[uint64]string{}
outer:
	for _, r := range rs {
		for _, s := range skip {
			if r.ID == s {
				continue outer
			}
		}
		for i, op := range r.Committed() {
			if prev, ok := at[i]; ok && prev != string(op) {
				t.Errorf("safety violation at index %d: %s has %s, %s has %s", i, who[i], prev, r.ID, op)
			}
			at[i], who[i] = string(op), r.ID
		}
	}
}
