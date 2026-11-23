package discovery

import (
	"airlink/internal/metadata"
	"errors"
	"net"
	"sync"
	"time"
)

var (
	ErrPeerManagerNodeIDEmpty  = errors.New("peer manager node ID is empty")
	ErrPeerManagerDiscoveryNil = errors.New("peer manager discovery is nil")
	ErrPeerManagerStarted      = errors.New("peer manager is already started")
)

type PeerRecord struct {
	Peer        Peer
	FirstSeenAt time.Time
	LastSeenAt  time.Time
}

type PeerManager struct {
	nodeId    metadata.PeerId
	discovery Discovery

	mu          sync.RWMutex
	discoveryMu sync.Mutex
	peers       map[metadata.PeerId]PeerRecord

	lifecycleMu sync.Mutex
	started     bool
	done        chan struct{}
}

func (m *PeerManager) Start() error {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()

	if m.started {
		return ErrPeerManagerStarted
	}

	m.discoveryMu.Lock()
	err := m.discovery.Start()
	m.discoveryMu.Unlock()

	if err != nil {
		return err
	}

	m.started = true
	return nil
}

func (m *PeerManager) Stop() error {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()

	if !m.started {
		return nil
	}

	m.discoveryMu.Lock()
	err := m.discovery.Stop()
	m.discoveryMu.Unlock()

	if err != nil {
		return err
	}

	m.started = false
	return nil
}

func (m *PeerManager) Discover() ([]PeerRecord, error) {
	m.discoveryMu.Lock()
	defer m.discoveryMu.Unlock()

	discoveredPeers, err := m.discovery.Discover()
	if err != nil {
		return m.GetPeers(), err
	}

	m.updatePeers(discoveredPeers, time.Now().UTC())

	return m.GetPeers(), nil
}

func (m *PeerManager) GetPeers() []PeerRecord {
	m.mu.RLock()

	records := make([]PeerRecord, 0, len(m.peers))
	for _, record := range m.peers {
		records = append(records, clonePeerRecord(record))
	}

	m.mu.RUnlock()

	return records
}

func (m *PeerManager) GetPeer(id metadata.PeerId) (PeerRecord, bool) {
	m.mu.RLock()
	record, ok := m.peers[id]
	m.mu.RUnlock()

	if !ok {
		return PeerRecord{}, false
	}

	return clonePeerRecord(record), true
}

func (m *PeerManager) updatePeers(discoveredPeers []Peer, now time.Time) {
	peers := make(map[metadata.PeerId]PeerRecord, len(discoveredPeers))

	for _, peer := range discoveredPeers {
		if peer.Id == "" || peer.Id == m.nodeId {
			continue
		}

		record := PeerRecord{
			Peer:        clonePeer(peer),
			FirstSeenAt: now,
			LastSeenAt:  now,
		}

		if previous, exists := m.peers[peer.Id]; exists {
			record.FirstSeenAt = previous.FirstSeenAt
		}

		peers[peer.Id] = record
	}

	m.mu.Lock()
	m.peers = peers
	m.mu.Unlock()
}

func clonePeerRecord(record PeerRecord) PeerRecord {
	record.Peer = clonePeer(record.Peer)
	return record
}

func clonePeer(peer Peer) Peer {
	if peer.Address != nil {
		address := *peer.Address
		address.IP = append(net.IP(nil), peer.Address.IP...)
		peer.Address = &address
	}

	return peer
}

func NewPeerManager(nodeID metadata.PeerId, discovery Discovery) (*PeerManager, error) {
	if nodeID == "" {
		return nil, ErrPeerManagerNodeIDEmpty
	}

	if discovery == nil {
		return nil, ErrPeerManagerDiscoveryNil
	}

	return &PeerManager{
		nodeId:    nodeID,
		discovery: discovery,
		peers:     make(map[metadata.PeerId]PeerRecord),
	}, nil
}
