package raftnode

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/hashicorp/raft"
	"github.com/pavann19/quorumkv/internal/diskhealth"
	"github.com/pavann19/quorumkv/internal/requestlimits"
	"github.com/pavann19/quorumkv/internal/store"
	"github.com/pavann19/quorumkv/internal/wal"
)

// StartOptions require an operator decision before creating a new cluster.
type StartOptions struct {
	ReadMode              string
	SnapshotFormatVersion uint32
	WALFormatVersion      uint32
	CommandFormatVersion  uint32
	RaftSettings          RaftSettings
	RequestLimits         requestlimits.Limits
	DiskPolicy            diskhealth.Policy
	StoreLimits           store.Limits
	ClearFatal            bool
	Bootstrap             bool
	BootstrapExpect       int
	ClusterID             string
}

type clusterManifest struct {
	Version               int          `json:"version"`
	SnapshotFormatVersion uint32       `json:"snapshot_format_version,omitempty"`
	WALFormatVersion      uint32       `json:"wal_format_version,omitempty"`
	CommandFormatVersion  uint32       `json:"command_format_version,omitempty"`
	ClusterID             string       `json:"cluster_id"`
	NodeID                string       `json:"node_id"`
	Peers                 []Peer       `json:"peers"`
	StoreLimits           store.Limits `json:"store_limits"`
}

func validateStart(self Peer, peers []Peer, options []StartOptions) (StartOptions, error) {
	if len(options) != 1 {
		return StartOptions{}, errors.New("raftnode: explicit cluster start options required")
	}
	o := options[0]
	if o.CommandFormatVersion > 1 || o.CommandFormatVersion == 1 && (o.WALFormatVersion != 1 || o.SnapshotFormatVersion != 1) {
		return o, errors.New("command format 1 requires WAL 1 and snapshot 1; unknown command formats refuse")
	}
	if o.WALFormatVersion > 1 {
		return o, errors.New("unsupported WAL format version")
	}
	if o.ReadMode == "" {
		o.ReadMode = "quorum"
	}
	if o.ReadMode != "quorum" && o.ReadMode != "barrier" {
		return o, errors.New("read mode must be quorum or barrier")
	}
	if o.SnapshotFormatVersion > 1 {
		return o, errors.New("unsupported snapshot format version")
	}
	if o.RaftSettings == (RaftSettings{}) {
		o.RaftSettings = DefaultRaftSettings()
	}
	if err := o.RaftSettings.Validate(); err != nil {
		return o, err
	}
	if o.RequestLimits == (requestlimits.Limits{}) {
		o.RequestLimits = requestlimits.Defaults()
	}
	if err := o.RequestLimits.Validate(); err != nil {
		return o, err
	}
	if o.DiskPolicy == (diskhealth.Policy{}) {
		o.DiskPolicy = diskhealth.DefaultPolicy()
	}
	if err := o.DiskPolicy.Validate(); err != nil {
		return o, err
	}
	if o.StoreLimits == (store.Limits{}) {
		o.StoreLimits = store.DefaultLimits()
	}
	if err := o.StoreLimits.Validate(); err != nil {
		return o, err
	}
	if len(o.ClusterID) == 0 || len(o.ClusterID) > 128 {
		return o, errors.New("raftnode: cluster-id required (1..128 bytes)")
	}
	for _, r := range o.ClusterID {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-') {
			return o, errors.New("raftnode: cluster-id must use ASCII letters, digits, dot, underscore or hyphen")
		}
	}
	if len(peers) == 0 || (o.Bootstrap && o.BootstrapExpect != len(peers)) {
		return o, errors.New("raftnode: bootstrap-expect must match nonempty peer list")
	}
	ids, addresses := map[string]bool{}, map[string]bool{}
	found := false
	for _, p := range peers {
		if p.ID == "" || ids[p.ID] || addresses[p.Addr] {
			return o, errors.New("raftnode: empty or duplicate peer")
		}
		if err := p.ValidateAddress(); err != nil {
			return o, fmt.Errorf("raftnode: peer address: %w", err)
		}
		ids[p.ID], addresses[p.Addr] = true, true
		if p == self {
			found = true
		}
	}
	if !found {
		return o, errors.New("raftnode: self must match a configured peer")
	}
	return o, nil
}
func desiredManifest(self Peer, peers []Peer, o StartOptions) clusterManifest {
	sorted := append([]Peer(nil), peers...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	version := 1
	if o.SnapshotFormatVersion == 1 {
		version = 2
	}
	if o.WALFormatVersion == 1 {
		version = 3
	}
	if o.CommandFormatVersion == 1 {
		version = 4
	}
	return clusterManifest{Version: version, CommandFormatVersion: o.CommandFormatVersion, WALFormatVersion: o.WALFormatVersion, SnapshotFormatVersion: o.SnapshotFormatVersion, ClusterID: o.ClusterID, NodeID: self.ID, Peers: sorted, StoreLimits: o.StoreLimits}
}
func validateManifest(dir string, self Peer, peers []Peer, o StartOptions) error {
	raw, err := os.ReadFile(filepath.Join(dir, "cluster.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var m clusterManifest
	if err = json.Unmarshal(raw, &m); err != nil {
		return fmt.Errorf("raftnode: invalid cluster manifest: %w", err)
	}
	if !reflect.DeepEqual(m, desiredManifest(self, peers, o)) {
		return errors.New("raftnode: cluster identity, node identity or peers differ from persisted manifest")
	}
	return nil
}
func persistManifest(dir string, self Peer, peers []Peer, o StartOptions, hasState bool) error {
	path := filepath.Join(dir, "cluster.json")
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// Existing state must be migrated through an explicit offline procedure;
	// never silently assign an identity to an existing cluster.
	if hasState {
		return errors.New("raftnode: existing Raft state lacks cluster.json; offline identity migration required")
	}
	raw, err := json.Marshal(desiredManifest(self, peers, o))
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, "cluster.*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(raw); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	return wal.SyncDirectory(dir)
}
func sameConfiguration(c raft.Configuration, peers []Peer) bool {
	if len(c.Servers) != len(peers) {
		return false
	}
	for _, p := range peers {
		found := false
		for _, s := range c.Servers {
			if string(s.ID) == p.ID && string(s.Address) == p.Addr && s.Suffrage == raft.Voter {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// ValidateAddress rejects wildcard or malformed advertised endpoints. Listening
// on a wildcard is a separate authenticated-listener decision, never peer identity.
func (p Peer) ValidateAddress() error {
	host, port, err := net.SplitHostPort(p.Addr)
	if err != nil {
		return err
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 || host == "" || strings.ContainsAny(host, "/\\@ \t\r\n") {
		return errors.New("advertised peer requires explicit host and port 1..65535")
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		return errors.New("wildcard addresses cannot identify advertised peers")
	}
	return nil
}
