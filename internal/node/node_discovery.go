package node

import (
	"airlink/internal/discovery"
	"context"
	"time"
)

func (n *P2PNode) GoOnline() error {
	return n.peerManager.Start()
}

func (n *P2PNode) GoOffline() error {
	return n.peerManager.Stop()
}

func (n *P2PNode) GetPeers() []discovery.PeerRecord {
	return n.peerManager.GetPeers()
}

func (n *P2PNode) StartPeerDiscovery(pollingFrequency time.Duration) {
	n.discoveryMu.Lock()

	if n.cancelPeerDiscovery != nil {
		n.cancelPeerDiscovery()
	}

	ctx, cancel := context.WithCancel(context.Background())
	n.cancelPeerDiscovery = cancel

	n.discoveryMu.Unlock()

	go func() {
		ticker := time.NewTicker(pollingFrequency)
		defer ticker.Stop()

	outer:
		for {
			select {
			case <-ticker.C:
				n.peerManager.Discover() // TODO if err then log
			case <-ctx.Done():
				break outer
			}
		}
	}()
}

func (n *P2PNode) StopPeerDiscovery() {
	n.discoveryMu.Lock()
	defer n.discoveryMu.Unlock()

	if n.cancelPeerDiscovery != nil {
		n.cancelPeerDiscovery()
		n.cancelPeerDiscovery = nil
	}
}
