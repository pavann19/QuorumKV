package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"time"

	"github.com/pavann19/quorumkv/internal/raftnode"
)

func runFormatMigration(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("migrate-format", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	source := flags.String("source", "", "offline source member directory")
	destination := flags.String("destination", "", "new disjoint member directory; must not exist")
	profile := flags.String("to-profile", "", "legacy or versioned")
	approval := flags.String("approve-sha256", "", "exact inspected source/destination/format fingerprint")
	timeout := flags.Duration("timeout", 2*time.Minute, "bounded offline inspection/conversion budget")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *source == "" || *destination == "" || *timeout <= 0 || *timeout > 30*time.Minute {
		return errors.New("source, destination, profile and a positive timeout <=30m required; no positional arguments")
	}
	var target raftnode.StorageProfile
	switch *profile {
	case "legacy":
	case "versioned":
		target = raftnode.StorageProfile{Snapshot: 1, WAL: 1, Command: 1}
	default:
		return errors.New("to-profile must be legacy or versioned")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	report, err := raftnode.ConvertMemberFormat(ctx, *source, *destination, target, *approval)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

func runIdentityMigration(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("migrate-identity", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	id := flags.String("id", "", "member ID")
	peers := flags.String("peers", "", "persisted voting membership")
	dir := flags.String("data-dir", "", "offline member copy")
	clusterID := flags.String("cluster-id", "", "operator-assigned legacy cluster identity")
	approval := flags.String("approve-sha256", "", "exact inspected source/config fingerprint; omit for inspection")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *id == "" || *peers == "" || *dir == "" || *clusterID == "" {
		return errors.New("migration requires id, peers, data-dir and cluster-id; no positional arguments")
	}
	parsed, err := parsePeers(*peers)
	if err != nil {
		return err
	}
	var self raftnode.Peer
	for _, peer := range parsed {
		if peer.ID == *id {
			self = peer
		}
	}
	report, err := raftnode.LegacyIdentityMigration(self, parsed, *dir, raftnode.StartOptions{ClusterID: *clusterID}, *approval)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}
