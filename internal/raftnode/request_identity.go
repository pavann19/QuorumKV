package raftnode

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

const RequestIDHeader = "quorumkv-request-id"

func newRequestID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	millis := uint64(time.Now().UnixMilli())
	for i := 5; i >= 0; i-- {
		id[i] = byte(millis)
		millis >>= 8
	}
	id[6] = 0x70 | (id[6] & 0x0f)
	id[8] = 0x80 | (id[8] & 0x3f)
	encoded := hex.EncodeToString(id[:])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:], nil
}
func validRequestID(id string) bool {
	if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' || id != strings.ToLower(id) {
		return false
	}
	raw, err := hex.DecodeString(strings.ReplaceAll(id, "-", ""))
	return err == nil && len(raw) == 16 && raw[6]>>4 == 7 && raw[8]>>6 == 2
}
func beginRequest(ctx context.Context) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	ids := md.Get(RequestIDHeader)
	if len(ids) > 1 || len(ids) == 1 && !validRequestID(ids[0]) {
		return "", rpcFailure(codes.InvalidArgument, "REQUEST_ID_INVALID", "not_submitted", nil)
	}
	var id string
	if len(ids) == 1 {
		id = ids[0]
	} else {
		var err error
		id, err = newRequestID()
		if err != nil {
			return "", rpcFailure(codes.Internal, "REQUEST_ID_UNAVAILABLE", "not_submitted", nil)
		}
	}
	// Transport cancellation may prevent delivery. A correlation ID is neither a
	// deduplication key nor proof that a submitted mutation did not take effect.
	grpc.SetHeader(ctx, metadata.Pairs(RequestIDHeader, id))
	grpc.SetTrailer(ctx, metadata.Pairs(RequestIDHeader, id))
	return id, nil
}
func finishRequest(id string, err error, mutation bool) error {
	if err == nil {
		return nil
	}
	proto := status.Convert(err).Proto()
	found := false
	for index, detail := range proto.Details {
		if !detail.MessageIs(&errdetails.ErrorInfo{}) {
			continue
		}
		var info errdetails.ErrorInfo
		if detail.UnmarshalTo(&info) != nil || info.Domain != "quorumkv" {
			continue
		}
		if info.Metadata == nil {
			info.Metadata = map[string]string{}
		}
		info.Metadata["request_id"] = id
		info.Metadata["next_action"] = nextAction(info.Reason, info.Metadata["outcome"])
		updated, e := anypb.New(&info)
		if e == nil {
			proto.Details[index] = updated
		}
		found = true
		if delay := retryDelay(info.Reason, info.Metadata["outcome"]); delay > 0 {
			retry, _ := anypb.New(&errdetails.RetryInfo{RetryDelay: durationpb.New(delay)})
			proto.Details = append(proto.Details, retry)
		}
	}
	if !found {
		outcome := "no_mutation"
		if mutation {
			outcome = "uncertain"
		}
		detail, _ := anypb.New(&errdetails.ErrorInfo{Domain: "quorumkv", Reason: "RPC_FAILURE", Metadata: map[string]string{"request_id": id, "outcome": outcome, "next_action": nextAction("RPC_FAILURE", outcome)}})
		proto.Details = append(proto.Details, detail)
		proto.Message = "quorumkv: RPC_FAILURE"
	}
	return status.FromProto(proto).Err()
}
func retryDelay(reason, outcome string) time.Duration {
	if outcome != "not_submitted" {
		return 0
	}
	switch reason {
	case "ADMISSION_FULL", "ENQUEUE_TIMEOUT", "LEADERSHIP_TRANSFER", "NO_LEADER", "NOT_LEADER":
		return 100 * time.Millisecond
	case "DISK_PRESSURE", "DISK_STATUS_UNKNOWN":
		return time.Second
	}
	return 0
}
func nextAction(reason, outcome string) string {
	if outcome == "uncertain" {
		return "reconcile"
	}
	if reason == "KEYSPACE_FULL" {
		return "review_capacity"
	}
	if retryDelay(reason, outcome) > 0 {
		return "bounded_retry_if_no_prior_uncertainty"
	}
	return "review_request"
}

func (s *KVServer) finishRPC(id string, err error, mutation bool) error {
	if err != nil && s.Node != nil && s.Node.Raft != nil {
		proto := status.Convert(err).Proto()
		for index, detail := range proto.Details {
			if !detail.MessageIs(&errdetails.ErrorInfo{}) {
				continue
			}
			var info errdetails.ErrorInfo
			if detail.UnmarshalTo(&info) != nil || info.Domain != "quorumkv" || (info.Reason != "NOT_LEADER" && info.Reason != "NO_LEADER") {
				continue
			}
			if info.Metadata == nil {
				info.Metadata = map[string]string{}
			}
			info.Metadata["term"] = s.Node.Raft.Stats()["term"]
			if leader := s.Node.LeaderAddr(); leader != "" {
				info.Metadata["leader"] = leader
			}
			updated, e := anypb.New(&info)
			if e == nil {
				proto.Details[index] = updated
			}
		}
		err = status.FromProto(proto).Err()
	}
	return finishRequest(id, err, mutation)
}
