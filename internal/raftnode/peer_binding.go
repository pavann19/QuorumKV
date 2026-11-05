package raftnode

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"strconv"
)

const PeerClusterHeader = "quorumkv-cluster-id"
const PeerProtocolHeader = "quorumkv-peer-protocol"
const PeerProtocolVersion = "2"
const PeerCommandProtocolVersion = "3"
const PeerSnapshotFormatHeader = "quorumkv-snapshot-format"
const PeerCommandFormatHeader = "quorumkv-command-format"

func commandBinding(formats []uint32) uint32 {
	if len(formats) == 0 {
		return 0
	}
	if len(formats) != 1 {
		return ^uint32(0)
	}
	return formats[0]
}
func peerProtocolFor(commandFormat uint32) string {
	if commandFormat == 0 {
		return PeerProtocolVersion
	}
	return PeerCommandProtocolVersion
}

func peerOutgoing(ctx context.Context, clusterID string, format uint32, commandFormats ...uint32) context.Context {
	commandFormat := commandBinding(commandFormats)
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md.Set(PeerClusterHeader, clusterID)
	md.Set(PeerProtocolHeader, peerProtocolFor(commandFormat))
	md.Set(PeerSnapshotFormatHeader, strconv.FormatUint(uint64(format), 10))
	md.Delete(PeerCommandFormatHeader)
	if commandFormat != 0 {
		md.Set(PeerCommandFormatHeader, strconv.FormatUint(uint64(commandFormat), 10))
	}
	return metadata.NewOutgoingContext(ctx, md)
}
func peerUnaryClient(clusterID string, format uint32, commandFormats ...uint32) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, request, response any, connection *grpc.ClientConn, invoke grpc.UnaryInvoker, options ...grpc.CallOption) error {
		return invokeSizedAppend(peerOutgoing(ctx, clusterID, format, commandFormats...), method, request, response, connection, invoke, options...)
	}
}
func peerStreamClient(clusterID string, format uint32, commandFormats ...uint32) grpc.StreamClientInterceptor {
	return func(ctx context.Context, descriptor *grpc.StreamDesc, connection *grpc.ClientConn, method string, streamer grpc.Streamer, options ...grpc.CallOption) (grpc.ClientStream, error) {
		if method != appendPipelineMethod {
			return streamer(peerOutgoing(ctx, clusterID, format, commandFormats...), descriptor, connection, method, options...)
		}
		streamCtx, cancel := context.WithCancel(peerOutgoing(ctx, clusterID, format, commandFormats...))
		stream, err := streamer(streamCtx, descriptor, connection, method, options...)
		if err != nil {
			cancel()
			return nil, err
		}
		return newSizedAppendStream(stream, cancel), nil
	}
}
func checkPeerBinding(ctx context.Context, clusterID string, format uint32, commandFormats ...uint32) error {
	commandFormat := commandBinding(commandFormats)
	md, _ := metadata.FromIncomingContext(ctx)
	clusters, versions := md.Get(PeerClusterHeader), md.Get(PeerProtocolHeader)
	formats := md.Get(PeerSnapshotFormatHeader)
	commands := md.Get(PeerCommandFormatHeader)
	commandValid := commandFormat == 0 && len(commands) == 0 || commandFormat == 1 && len(commands) == 1 && commands[0] == "1"
	if !commandValid || len(clusters) != 1 || clusters[0] != clusterID || len(versions) != 1 || versions[0] != peerProtocolFor(commandFormat) || len(formats) != 1 || formats[0] != strconv.FormatUint(uint64(format), 10) {
		return status.Error(codes.PermissionDenied, "peer cluster/protocol binding rejected")
	}
	return nil
}

// Gate the adopted transport at registration, so callers cannot accidentally
// omit a separate server interceptor. TLS identity/authorization interceptors
// remain in place; cluster metadata is an additional compatibility boundary,
// never a substitute for authenticated peer authority.
type peerRegistrar struct {
	registrar     grpc.ServiceRegistrar
	clusterID     string
	format        uint32
	commandFormat uint32
}

func (p peerRegistrar) RegisterService(original *grpc.ServiceDesc, implementation any) {
	descriptor := *original
	descriptor.Methods = append([]grpc.MethodDesc(nil), original.Methods...)
	descriptor.Streams = append([]grpc.StreamDesc(nil), original.Streams...)
	for index, method := range original.Methods {
		handler := method.Handler
		descriptor.Methods[index].Handler = func(server any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			if err := checkPeerBinding(ctx, p.clusterID, p.format, p.commandFormat); err != nil {
				return nil, err
			}
			return handler(server, ctx, decode, interceptor)
		}
	}
	for index, stream := range original.Streams {
		handler := stream.Handler
		descriptor.Streams[index].Handler = func(server any, stream grpc.ServerStream) error {
			if err := checkPeerBinding(stream.Context(), p.clusterID, p.format, p.commandFormat); err != nil {
				return err
			}
			return handler(server, stream)
		}
	}
	p.registrar.RegisterService(&descriptor, implementation)
}
