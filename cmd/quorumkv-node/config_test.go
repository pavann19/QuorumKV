package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStrictNodeConfiguration(t *testing.T) {
	cases := []string{`null`, `[]`, `{"id":"one","ID":"two"}`, `{"requests":null}`, `{"unknown":true}`, `{"requests":{"typo":1}}`, `{} {}`, strings.Repeat(" ", 65537)}
	for _, raw := range cases {
		path := filepath.Join(t.TempDir(), "node.json")
		os.WriteFile(path, []byte(raw), 0600)
		if _, err := readNodeConfig(path); err == nil {
			t.Fatalf("accepted invalid config %.60s", raw)
		}
	}
	path := filepath.Join(t.TempDir(), "node.json")
	os.WriteFile(path, []byte(`{"id":"one","cluster_id":"test","peers":"one=127.0.0.1:19001","data_dir":"fixture","requests":{"max_inflight":2,"timeout_ms":400}}`), 0600)
	cfg, err := readNodeConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = cfg.validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.Requests.Inflight != 2 || cfg.Requests.KeyBytes != 4096 || cfg.Requests.TimeoutMS != 400 {
		t.Fatalf("defaults overlay failed %+v", cfg.Requests)
	}
	raw, _ := json.Marshal(cfg)
	var same nodeConfig
	json.Unmarshal(raw, &same)
	if cfg.hash("identity") != same.hash("identity") {
		t.Fatal("hash not reproducible")
	}
	same.Disk.MaxUsedPercent = 95
	if cfg.hash("identity") == same.hash("identity") {
		t.Fatal("effective disk config not hashed")
	}
	if cfg.hash("identity") == cfg.hash("rotated") {
		t.Fatal("identity config hash not included")
	}
}
func TestNodeConfigurationSemanticValidation(t *testing.T) {
	good := defaultNodeConfig()
	good.ID = "one"
	good.ClusterID = "test"
	good.Peers = "one=127.0.0.1:19001"
	good.DataDir = "fixture"
	for _, mutate := range []func(*nodeConfig){func(c *nodeConfig) { c.CommandFormatVersion = 2 }, func(c *nodeConfig) { c.CommandFormatVersion = 1 }, func(c *nodeConfig) { c.WALFormatVersion = 2 }, func(c *nodeConfig) { c.Requests.WriteReadGraceMS = 26 }, func(c *nodeConfig) { c.ReadMode = "unsafe" }, func(c *nodeConfig) { c.Requests.TimeoutMS = 0 }, func(c *nodeConfig) { c.Requests.Inflight = 65 }, func(c *nodeConfig) { c.Store.MaxBytes = -1 }, func(c *nodeConfig) { c.Disk.MaxUsedPercent = 100 }, func(c *nodeConfig) { c.AdminAddr = "0.0.0.0:19002" }, func(c *nodeConfig) { c.AdminAddr = "127.0.0.1:19001" }, func(c *nodeConfig) { c.Peers = "one=127.0.0.1:invalid" }, func(c *nodeConfig) { c.ID = "missing" }} {
		cfg := good
		mutate(&cfg)
		if cfg.validate() == nil {
			t.Fatalf("invalid settings accepted %+v", cfg)
		}
	}
}

func TestListenerAliasesAndCanonicalConfig(t *testing.T) {
	for _, pair := range [][2]string{{"0.0.0.0:7070", "127.0.0.1:7070"}, {"127.0.0.1:07070", "127.0.0.1:7070"}, {"[::ffff:127.0.0.1]:7070", "127.0.0.1:7070"}} {
		if !sameListener(pair[0], pair[1]) {
			t.Fatalf("overlap missed: %v", pair)
		}
	}
	cfg := defaultNodeConfig()
	cfg.ID = "one"
	cfg.ClusterID = "cfg"
	cfg.DataDir = "fixture"
	cfg.Peers = "two=127.0.0.1:7072,one=127.0.0.1:7070,three=127.0.0.1:7073"
	equivalent := cfg
	equivalent.Peers = "one=127.0.0.1:7070,three=127.0.0.1:7073,two=127.0.0.1:7072"
	equivalent.ListenAddr = "127.0.0.1:7070"
	if cfg.hash("") != equivalent.hash("") {
		t.Fatal("equivalent peer order/listen default changed canonical hash")
	}
	cfg.ListenAddr = "0.0.0.0:7070"
	if cfg.validate() == nil {
		t.Fatal("plaintext wildcard listen accepted")
	}
	cfg.Identity = "operator-identity.json"
	cfg.AdminAddr = "127.0.0.1:07070"
	if cfg.validate() == nil {
		t.Fatal("wildcard listener/admin overlap accepted")
	}
}
