package common

import (
	"context"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"wb/internal/fault"
)

// Counters for messages this process sent to other replicas (one replica per process).
var Sent, SentBytes atomic.Int64

// outbound counts every message and adds the injected one-way delay to both the
// request and its response, as a real network would: Raft needs the reply to commit,
// so delaying only requests would understate its latency.
func outbound(inj *fault.Injector) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		d := inj.Delay()
		time.Sleep(d)
		Sent.Add(1)
		if m, ok := req.(proto.Message); ok {
			SentBytes.Add(int64(proto.Size(m)))
		}
		err := invoker(ctx, method, req, reply, cc, opts...)
		time.Sleep(d)
		return err
	}
}

// Inbound drops a received message when the fault injector says this replica cannot
// hear its sender (crash, omission, or a partition that excludes the sender).
func Inbound(inj *fault.Injector) grpc.ServerOption {
	return grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		from := ""
		if m, ok := req.(interface{ GetSender() string }); ok {
			from = m.GetSender()
		}
		if inj.Drop(from) {
			return nil, status.Error(codes.Unavailable, "fault: dropped")
		}
		return h(ctx, req)
	})
}
