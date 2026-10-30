package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"wb/internal/common"
	"wb/internal/fault"
	"wb/internal/pbft"
	"wb/internal/raft"
	"wb/internal/server"
	"wb/pb"
)

func main() {
	id := flag.Int("id", 0, "replica index")
	n := flag.Int("n", 4, "cluster size")
	proto := flag.String("proto", "raft", "raft|pbft")
	fast := flag.Bool("fast", false, "raft: short election timeout (wins first election)")
	keys := flag.String("keys", "keys", "directory written by cmd/keygen")
	flag.Parse()

	rid := fmt.Sprintf("r%d", *id)
	auth, err := common.LoadAuth(*keys, rid)
	if err != nil {
		log.Fatalf("loading keys (run: go run ./cmd/keygen -n %d): %v", *n, err)
	}
	inj := fault.New()
	srv := &server.Server{ID: rid, Proto: *proto, Inj: inj}
	srv.Init()
	conns := common.Dial(rid, *n)

	gs := grpc.NewServer(grpc.UnaryInterceptor(
		func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			if inj.Drop() { // inbound crash/omission
				return nil, status.Error(codes.Unavailable, "fault: dropped")
			}
			return h(ctx, req)
		}))

	switch *proto {
	case "raft":
		nd := raft.New(rid, *n, conns, auth, inj, srv.OnCommit, *fast)
		pb.RegisterRaftServer(gs, nd)
		srv.Cons = nd
		nd.Run()
	case "pbft":
		nd := pbft.New(rid, *n, conns, auth, inj, srv.OnCommit)
		pb.RegisterPbftServer(gs, nd)
		srv.Cons = nd
	default:
		log.Fatal("unknown proto")
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", common.BasePort+*id))
	if err != nil {
		log.Fatal(err)
	}
	go gs.Serve(lis)
	log.Printf("%s up (%s, n=%d) grpc=%d http=%d", rid, *proto, *n, common.BasePort+*id, 8000+*id)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", 8000+*id), srv.Handler()))
}