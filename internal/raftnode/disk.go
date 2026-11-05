package raftnode

import "github.com/pavann19/quorumkv/internal/diskhealth"

func (n *Node) DiskPolicy() diskhealth.Policy { return n.diskPolicy }
func (n *Node) DiskStatus() (diskhealth.Status, error) {
	if n.diskProbe == nil {
		return diskhealth.Status{Degraded: true}, diskhealth.ErrUnknown
	}
	space, err := n.diskProbe(n.dataDir)
	if err != nil {
		return diskhealth.Status{Degraded: true}, diskhealth.ErrUnknown
	}
	return diskhealth.Evaluate(space, n.diskPolicy)
}
func (n *Node) admitDisk() error { _, err := n.DiskStatus(); return err }
