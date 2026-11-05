package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/pavann19/quorumkv/internal/diskhealth"
	"github.com/pavann19/quorumkv/internal/raftnode"
	"github.com/pavann19/quorumkv/internal/requestlimits"
	"github.com/pavann19/quorumkv/internal/store"
)

// Bootstrap and fatal clearance are intentionally CLI-only one-time operations.
// Configuration contains paths to identity material, never private key bytes.
type nodeConfig struct {
	ReadMode              string                `json:"read_mode"`
	SnapshotFormatVersion uint32                `json:"snapshot_format_version"`
	CommandFormatVersion  uint32                `json:"command_format_version"`
	WALFormatVersion      uint32                `json:"wal_format_version"`
	ID                    string                `json:"id"`
	Peers                 string                `json:"peers"`
	DataDir               string                `json:"data_dir"`
	ClusterID             string                `json:"cluster_id"`
	Identity              string                `json:"identity"`
	ListenAddr            string                `json:"listen_addr"`
	AdminAddr             string                `json:"admin_addr"`
	Store                 store.Limits          `json:"store"`
	Disk                  diskhealth.Policy     `json:"disk"`
	Raft                  raftnode.RaftSettings `json:"raft"`
	Requests              requestlimits.Limits  `json:"requests"`
}

func defaultNodeConfig() nodeConfig {
	return nodeConfig{ReadMode: "quorum", Raft: raftnode.DefaultRaftSettings(), Store: store.DefaultLimits(), Disk: diskhealth.DefaultPolicy(), Requests: requestlimits.Defaults()}
}
func readNodeConfig(path string) (nodeConfig, error) {
	cfg := defaultNodeConfig()
	if path == "" {
		return cfg, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return cfg, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, 65537))
	if err != nil {
		return cfg, err
	}
	if len(raw) > 65536 {
		return cfg, errors.New("node configuration exceeds 64 KiB")
	}
	if len(bytes.TrimSpace(raw)) == 0 || bytes.TrimSpace(raw)[0] != '{' {
		return cfg, errors.New("node configuration must be a JSON object")
	}
	if err := validateConfigTokens(json.NewDecoder(bytes.NewReader(raw)), 0); err != nil {
		return cfg, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&cfg); err != nil {
		return cfg, err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return cfg, errors.New("trailing node configuration data")
	}
	return cfg, nil
}
func (c nodeConfig) validate() error {
	if c.CommandFormatVersion > 1 || c.CommandFormatVersion == 1 && (c.WALFormatVersion != 1 || c.SnapshotFormatVersion != 1) {
		return errors.New("command format 1 requires WAL 1 and snapshot 1")
	}
	if c.WALFormatVersion > 1 {
		return errors.New("unsupported wal_format_version")
	}
	if c.ReadMode != "quorum" && c.ReadMode != "barrier" {
		return errors.New("read_mode must be quorum or barrier")
	}
	if c.SnapshotFormatVersion > 1 {
		return errors.New("unsupported snapshot_format_version")
	}
	if c.ID == "" || c.Peers == "" || c.DataDir == "" || c.ClusterID == "" {
		return errors.New("id, peers, data_dir and cluster_id required")
	}
	peers, err := parsePeers(c.Peers)
	if err != nil {
		return err
	}
	found := false
	for _, p := range peers {
		if err := p.ValidateAddress(); err != nil {
			return err
		}
		if p.ID == c.ID {
			found = true
		}
		_, port, _ := net.SplitHostPort(p.Addr)
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65535 {
			return errors.New("peer ports must be 1..65535")
		}
		if c.AdminAddr != "" && p.Addr == c.AdminAddr {
			return errors.New("admin and peer listeners must differ")
		}
	}
	if !found {
		return errors.New("configured id is not a peer")
	}
	listen := c.normalized().ListenAddr
	host, port, err := net.SplitHostPort(listen)
	number, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || number < 1 || number > 65535 || host == "" {
		return errors.New("listen_addr requires an explicit host and port 1..65535")
	}
	if c.Identity == "" {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return errors.New("plaintext listen_addr requires literal loopback")
		}
	}
	if c.AdminAddr != "" && sameListener(listen, c.AdminAddr) {
		return errors.New("admin and node listeners overlap")
	}
	if c.AdminAddr != "" {
		host, port, err := net.SplitHostPort(c.AdminAddr)
		ip := net.ParseIP(host)
		number, portErr := strconv.Atoi(port)
		if err != nil || portErr != nil || number < 1 || number > 65535 || ip == nil || !ip.IsLoopback() {
			return errors.New("admin listener must have literal loopback address and valid port")
		}
	}
	if err := c.Store.Validate(); err != nil {
		return err
	}
	if err := c.Disk.Validate(); err != nil {
		return err
	}
	if err := c.Raft.Validate(); err != nil {
		return err
	}
	return c.Requests.Validate()
}
func (c nodeConfig) hash(identityHash string) [32]byte {
	c = c.normalized()
	raw, _ := json.Marshal(struct {
		Config         nodeConfig `json:"configuration"`
		IdentitySHA256 string     `json:"identity_sha256"`
	}{c, identityHash})
	return sha256.Sum256(raw)
}

func validateConfigTokens(d *json.Decoder, depth int) error {
	if depth > 16 {
		return errors.New("node configuration nesting exceeds 16")
	}
	token, err := d.Token()
	if err != nil {
		return err
	}
	if token == nil {
		return errors.New("null node configuration values are not supported")
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok {
				return errors.New("invalid configuration field")
			}
			normalized := strings.ToLower(name)
			if seen[normalized] {
				return errors.New("duplicate configuration field")
			}
			seen[normalized] = true
			if err := validateConfigTokens(d, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := validateConfigTokens(d, depth+1); err != nil {
				return err
			}
		}
	default:
		return errors.New("unexpected configuration delimiter")
	}
	_, err = d.Token()
	return err
}

func (c nodeConfig) normalized() nodeConfig {
	peers, err := parsePeers(c.Peers)
	if err != nil {
		return c
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].ID < peers[j].ID })
	parts := make([]string, len(peers))
	for i, peer := range peers {
		if c.ListenAddr == "" && peer.ID == c.ID {
			c.ListenAddr = peer.Addr
		}
		parts[i] = peer.ID + "=" + peer.Addr
	}
	c.Peers = strings.Join(parts, ",")
	return c
}

func sameListener(a, b string) bool {
	hostA, portA, errA := net.SplitHostPort(a)
	hostB, portB, errB := net.SplitHostPort(b)
	if errA != nil || errB != nil {
		return false
	}
	numberA, eA := strconv.Atoi(portA)
	numberB, eB := strconv.Atoi(portB)
	if eA != nil || eB != nil || numberA != numberB {
		return false
	}
	ipA, ipB := net.ParseIP(hostA), net.ParseIP(hostB)
	if ipA != nil && ipB != nil {
		return ipA.IsUnspecified() || ipB.IsUnspecified() || ipA.Equal(ipB)
	}
	return strings.EqualFold(hostA, hostB)
}
