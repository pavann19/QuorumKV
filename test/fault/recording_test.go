package fault

import (
	"context"
	"net"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pavann19/quorumkv/internal/kv"
	"github.com/pavann19/quorumkv/internal/store"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func observed(history []Op) []Op {
	result := append([]Op(nil), history...)
	for i := range result {
		op := &result[i]
		op.OperationID, op.AttemptID, op.Attempt, op.Node = int64(i+1), int64(i+1), 1, "node1"
		op.Acknowledged = op.Err == ""
		if op.Err == "" {
			op.Outcome = "acknowledged"
		} else if op.Kind == "get" {
			op.Outcome = "failed_read"
		} else {
			op.Outcome = "uncertain"
		}
	}
	return result
}

func TestWorkloadReadsTheKeysItWrites(t *testing.T) {
	for _, size := range []int{3, 4} {
		writes, reads := map[string]bool{}, map[string]bool{}
		for client := 0; client < size; client++ {
			for i := 0; i < 8; i++ {
				kind, key := workloadOperation(client, i, keys(size))
				if kind == "put" {
					writes[key] = true
				}
				if kind == "get" {
					reads[key] = true
				}
			}
		}
		if len(writes) != size || len(reads) != size {
			t.Fatalf("uncovered keys: writes=%v reads=%v", writes, reads)
		}
		for key := range writes {
			if !reads[key] {
				t.Fatalf("write-only key %q", key)
			}
		}
	}
	old := observed([]Op{put(0, "write-only", "v", 1, 2), get(0, "read-only", false, "", 3, 4)})
	if validateWorkloadCoverage(old) == nil {
		t.Fatal("old disjoint-key workload accepted")
	}
}

func TestHistoryRejectsLostOrContradictoryAttempts(t *testing.T) {
	base := observed([]Op{put(0, "k", "v", 1, 2), get(0, "k", true, "v", 3, 4)})
	cases := map[string]func([]Op){
		"gap in attempt IDs":          func(h []Op) { h[1].AttemptID = 3 },
		"duplicate ID":                func(h []Op) { h[1].AttemptID = h[0].AttemptID },
		"missing retry":               func(h []Op) { h[1].Attempt = 2 },
		"missing acknowledgement":     func(h []Op) { h[0].Acknowledged = false },
		"uncertain acknowledgement":   func(h []Op) { h[0].Err = "timeout"; h[0].Outcome = "uncertain" },
		"backwards time":              func(h []Op) { h[1].ReturnNS = h[1].CallNS },
		"retry after acknowledgement": func(h []Op) { h[1].OperationID = h[0].OperationID; h[1].Attempt = 2 },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			h := append([]Op(nil), base...)
			change(h)
			if ValidateHistory(h) == nil {
				t.Fatal("invalid history accepted")
			}
		})
	}
}

func TestDeleteAndLateRetryControls(t *testing.T) {
	deleted := observed([]Op{put(0, "k", "v", 1, 2), {ClientID: 0, Kind: "delete", Key: "k", CallNS: 3, ReturnNS: 4}, get(0, "k", false, "", 5, 6)})
	if err := CheckHistory(deleted); err != nil {
		t.Fatal(err)
	}
	deleted[2].Found, deleted[2].GotValue = true, "v"
	if CheckHistory(deleted) == nil {
		t.Fatal("read resurrected acknowledged delete")
	}
	late := observed([]Op{failedPut(0, "k", "old", 1, 2), put(0, "k", "old", 3, 4), put(1, "k", "new", 5, 6), get(1, "k", true, "old", 7, 8)})
	late[1].OperationID, late[1].Attempt = late[0].OperationID, 2
	if err := CheckHistory(late); err != nil {
		t.Fatalf("late uncertain attempt missing from model: %v", err)
	}
	late[3].GotValue = "never-written"
	if CheckHistory(late) == nil {
		t.Fatal("uncertain attempt excused a value never written")
	}
}

func TestRecorderPreservesAppliedWriteWithLostReply(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "store.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	var first atomic.Bool
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		response, err := handler(ctx, req)
		if err == nil && strings.HasSuffix(info.FullMethod, "/Put") && first.CompareAndSwap(false, true) {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return response, err
	}))
	pb.RegisterKVServer(server, kv.New(st))
	lis := bufconn.Listen(1 << 20)
	go server.Serve(lis)
	t.Cleanup(func() { server.Stop(); lis.Close() })
	conn, err := grpc.NewClient("passthrough:///observed", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	client := pb.NewKVClient(conn)
	r := NewRecorder()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := r.Execute(ctx, 0, "put", "k", "v", []string{"node1"}, []pb.KVClient{client}, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := r.ExecuteOnce(ctx, 0, "get", "k", "", "node1", client); err != nil {
		t.Fatal(err)
	}
	history := r.History()
	if len(history) != 3 || history[0].Outcome != "uncertain" || history[1].Attempt != 2 || !history[1].Acknowledged || history[0].OperationID != history[1].OperationID {
		t.Fatalf("lost attempts: %+v", history)
	}
	CheckAndCommit(t, "lost_reply", history)
}
