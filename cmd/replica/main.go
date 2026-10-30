package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc"

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
	batchMax := flag.Int("batch", 32, "most ops per consensus entry (1 disables batching)")
	batchWait := flag.Duration("batch-wait", 2*time.Millisecond, "longest the leader holds an op for a batch")
	startFault := flag.String("fault", "", "fault mode from the start (e.g. equivocate, partition)")
	peers := flag.String("peers", "", "with -fault partition: comma-separated replicas this one can reach")
	delay := flag.Duration("delay", 0, "one-way delay added to every message this replica sends")
	flag.Parse()

	rid := fmt.Sprintf("r%d", *id)
	auth, err := common.LoadAuth(*keys, rid)
	if err != nil {
		log.Fatalf("loading keys (run: go run ./cmd/keygen -n %d): %v", *n, err)
	}
	inj := fault.New()
	srv := &server.Server{ID: rid, Proto: *proto, N: *n, Inj: inj, BatchMax: *batchMax, BatchWait: *batchWait}
	srv.Init()
	switch {
	case *startFault == "partition":
		inj.SetPartition(strings.Split(*peers, ","))
	case *startFault != "":
		inj.Set(fault.Mode(*startFault), 0)
	}
	inj.SetDelay(*delay)
	conns := common.Dial(rid, *n, inj)
	gs := grpc.NewServer(common.Inbound(inj))

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