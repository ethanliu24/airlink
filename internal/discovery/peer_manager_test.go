package discovery

import (
	"airlink/internal/metadata"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockPeerDiscovery struct {
	mu sync.Mutex

	startErr    error
	stopErr     error
	discoverErr error

	peers []Peer

	startCalls    int
	stopCalls     int
	discoverCalls int
}

func (d *mockPeerDiscovery) Start() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.startCalls++
	return d.startErr
}

func (d *mockPeerDiscovery) Stop() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.stopCalls++
	return d.stopErr
}

func (d *mockPeerDiscovery) Discover() ([]Peer, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.discoverCalls++

	peers := append([]Peer(nil), d.peers...)
	return peers, d.discoverErr
}

func (d *mockPeerDiscovery) setStartErr(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.startErr = err
}

func (d *mockPeerDiscovery) setStopErr(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stopErr = err
}

func (d *mockPeerDiscovery) setDiscoverErr(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.discoverErr = err
}

func (d *mockPeerDiscovery) setPeers(peers []Peer) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.peers = append([]Peer(nil), peers...)
}

func (d *mockPeerDiscovery) getCallCounts() (int, int, int) {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.startCalls, d.stopCalls, d.discoverCalls
}

func newTestPeerManager(
	t *testing.T,
	discovery *mockPeerDiscovery,
) *PeerManager {
	t.Helper()

	manager, err := NewPeerManager(metadata.PeerId("local-node"), discovery)
	require.NoError(t, err)

	return manager
}

func TestNewPeerManager(t *testing.T) {
	t.Run("creates manager", func(t *testing.T) {
		discovery := &mockPeerDiscovery{}

		manager, err := NewPeerManager(metadata.PeerId("local-node"), discovery)

		require.NoError(t, err)
		require.NotNil(t, manager)

		assert.Equal(t, metadata.PeerId("local-node"), manager.nodeId)
		assert.Same(t, discovery, manager.discovery)
		assert.Empty(t, manager.GetPeers())
	})

	t.Run("rejects empty node ID", func(t *testing.T) {
		manager, err := NewPeerManager("", &mockPeerDiscovery{})

		assert.Nil(t, manager)
		assert.ErrorIs(t, err, ErrPeerManagerNodeIDEmpty)
	})

	t.Run("rejects nil discovery", func(t *testing.T) {
		manager, err := NewPeerManager(metadata.PeerId("local-node"), nil)

		assert.Nil(t, manager)
		assert.ErrorIs(t, err, ErrPeerManagerDiscoveryNil)
	})
}

func TestPeerManagerStartStop(t *testing.T) {
	discovery := &mockPeerDiscovery{}
	manager := newTestPeerManager(t, discovery)

	require.NoError(t, manager.Start())

	assert.ErrorIs(t, manager.Start(), ErrPeerManagerStarted)

	// Starting the manager should not automatically perform discovery.
	startCalls, _, discoverCalls := discovery.getCallCounts()
	assert.Equal(t, 1, startCalls)
	assert.Zero(t, discoverCalls)

	require.NoError(t, manager.Stop())

	// Stopping twice should be safe.
	require.NoError(t, manager.Stop())

	startCalls, stopCalls, discoverCalls := discovery.getCallCounts()
	assert.Equal(t, 1, startCalls)
	assert.Equal(t, 1, stopCalls)
	assert.Zero(t, discoverCalls)
}

func TestPeerManagerStartReturnsDiscoveryError(t *testing.T) {
	startErr := errors.New("discovery start failed")
	discovery := &mockPeerDiscovery{}
	discovery.setStartErr(startErr)

	manager := newTestPeerManager(t, discovery)

	err := manager.Start()
	assert.ErrorIs(t, err, startErr)

	// A failed start should not leave the manager marked as started.
	discovery.setStartErr(nil)

	require.NoError(t, manager.Start())
	require.NoError(t, manager.Stop())
}

func TestPeerManagerStopCanRetryAfterDiscoveryError(t *testing.T) {
	stopErr := errors.New("discovery stop failed")
	discovery := &mockPeerDiscovery{}
	manager := newTestPeerManager(t, discovery)

	require.NoError(t, manager.Start())

	discovery.setStopErr(stopErr)

	require.ErrorIs(t, manager.Stop(), stopErr)

	// A failed stop should leave the manager in a state
	// where Stop can be retried.
	discovery.setStopErr(nil)

	require.NoError(t, manager.Stop())

	_, stopCalls, _ := discovery.getCallCounts()
	assert.Equal(t, 2, stopCalls)
}

func TestPeerManagerDiscoverFiltersSelfAndDeduplicatesPeers(t *testing.T) {
	discovery := &mockPeerDiscovery{}
	discovery.setPeers([]Peer{
		{
			Id:   metadata.PeerId("local-node"),
			Name: "Self",
		},
		{
			Id:   metadata.PeerId("peer-1"),
			Name: "OldName",
		},
		{
			Id:   metadata.PeerId("peer-1"),
			Name: "NewName",
		},
		{
			Id:   metadata.PeerId("peer-2"),
			Name: "AnotherPeer",
		},
		{
			Id:   "",
			Name: "InvalidPeer",
		},
	})

	manager := newTestPeerManager(t, discovery)

	records, err := manager.Discover()

	require.NoError(t, err)
	require.Len(t, records, 2)

	peer1, exists := manager.GetPeer(metadata.PeerId("peer-1"))
	require.True(t, exists)
	assert.Equal(t, "NewName", peer1.Peer.Name)

	_, exists = manager.GetPeer(metadata.PeerId("peer-2"))
	assert.True(t, exists)

	_, exists = manager.GetPeer(metadata.PeerId("local-node"))
	assert.False(t, exists)

	for _, record := range records {
		assert.False(t, record.FirstSeenAt.IsZero())
		assert.False(t, record.LastSeenAt.IsZero())
	}
}

func TestPeerManagerDiscoverReplacesSnapshot(t *testing.T) {
	discovery := &mockPeerDiscovery{}
	manager := newTestPeerManager(t, discovery)

	discovery.setPeers([]Peer{
		{
			Id:   metadata.PeerId("peer-1"),
			Name: "Peer 1",
		},
		{
			Id:   metadata.PeerId("peer-2"),
			Name: "Peer 2",
		},
	})

	records, err := manager.Discover()

	require.NoError(t, err)
	require.Len(t, records, 2)

	// Peer 1 is no longer returned by discovery.
	discovery.setPeers([]Peer{
		{
			Id:   metadata.PeerId("peer-2"),
			Name: "Peer 2",
		},
		{
			Id:   metadata.PeerId("peer-3"),
			Name: "Peer 3",
		},
	})

	records, err = manager.Discover()

	require.NoError(t, err)
	require.Len(t, records, 2)

	_, exists := manager.GetPeer(metadata.PeerId("peer-1"))
	assert.False(t, exists)

	_, exists = manager.GetPeer(metadata.PeerId("peer-2"))
	assert.True(t, exists)

	_, exists = manager.GetPeer(metadata.PeerId("peer-3"))
	assert.True(t, exists)
}

func TestPeerManagerDiscoverUpdatesExistingPeer(t *testing.T) {
	manager := newTestPeerManager(t, &mockPeerDiscovery{})

	firstSeen := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	lastSeen := firstSeen.Add(5 * time.Second)

	manager.updatePeers([]Peer{
		{
			Id:   metadata.PeerId("peer-1"),
			Name: "OldName",
			Address: &net.UDPAddr{
				IP:   net.ParseIP("192.0.2.10"),
				Port: 1111,
			},
		},
	}, firstSeen)

	manager.updatePeers([]Peer{
		{
			Id:   metadata.PeerId("peer-1"),
			Name: "NewName",
			Address: &net.UDPAddr{
				IP:   net.ParseIP("192.0.2.20"),
				Port: 2222,
			},
		},
	}, lastSeen)

	record, exists := manager.GetPeer(metadata.PeerId("peer-1"))

	require.True(t, exists)
	assert.Equal(t, "NewName", record.Peer.Name)
	assert.Equal(t, net.ParseIP("192.0.2.20"), record.Peer.Address.IP)
	assert.Equal(t, 2222, record.Peer.Address.Port)
	assert.Equal(t, firstSeen, record.FirstSeenAt)
	assert.Equal(t, lastSeen, record.LastSeenAt)
}

func TestPeerManagerDiscoverPreservesPeersOnError(t *testing.T) {
	discovery := &mockPeerDiscovery{}
	manager := newTestPeerManager(t, discovery)

	discovery.setPeers([]Peer{
		{
			Id:   metadata.PeerId("peer-1"),
			Name: "Peer",
		},
	})

	records, err := manager.Discover()

	require.NoError(t, err)
	require.Len(t, records, 1)

	discovery.setDiscoverErr(errors.New("discovery failed"))

	records, err = manager.Discover()

	require.Error(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, metadata.PeerId("peer-1"), records[0].Peer.Id)
	assert.Equal(t, "Peer", records[0].Peer.Name)
}

func TestPeerManagerGetPeer(t *testing.T) {
	manager := newTestPeerManager(t, &mockPeerDiscovery{})

	manager.updatePeers([]Peer{
		{
			Id:   metadata.PeerId("peer-1"),
			Name: "Peer",
		},
	}, time.Now())

	record, exists := manager.GetPeer(metadata.PeerId("peer-1"))

	require.True(t, exists)
	assert.Equal(t, metadata.PeerId("peer-1"), record.Peer.Id)

	_, exists = manager.GetPeer(metadata.PeerId("missing"))
	assert.False(t, exists)
}

func TestPeerManagerGetPeersReturnsCopy(t *testing.T) {
	manager := newTestPeerManager(t, &mockPeerDiscovery{})

	manager.updatePeers([]Peer{
		{
			Id:   metadata.PeerId("peer-1"),
			Name: "Original",
			Address: &net.UDPAddr{
				IP:   net.ParseIP("192.0.2.10"),
				Port: 1111,
			},
		},
	}, time.Now())

	records := manager.GetPeers()
	require.Len(t, records, 1)

	// Mutating returned records must not mutate the registry.
	records[0].Peer.Name = "Modified"
	records[0].Peer.Address.IP[0] = 255
	records[0].Peer.Address.Port = 9999

	stored, exists := manager.GetPeer(metadata.PeerId("peer-1"))

	require.True(t, exists)
	assert.Equal(t, "Original", stored.Peer.Name)
	assert.Equal(t, net.ParseIP("192.0.2.10"), stored.Peer.Address.IP)
	assert.Equal(t, 1111, stored.Peer.Address.Port)
}

func TestPeerManagerGetPeerReturnsCopy(t *testing.T) {
	manager := newTestPeerManager(t, &mockPeerDiscovery{})

	manager.updatePeers([]Peer{
		{
			Id:   metadata.PeerId("peer-1"),
			Name: "Original",
			Address: &net.UDPAddr{
				IP:   net.ParseIP("192.0.2.10"),
				Port: 1111,
			},
		},
	}, time.Now())

	record, exists := manager.GetPeer(metadata.PeerId("peer-1"))
	require.True(t, exists)

	record.Peer.Name = "Modified"
	record.Peer.Address.IP[0] = 255
	record.Peer.Address.Port = 9999

	stored, exists := manager.GetPeer(metadata.PeerId("peer-1"))

	require.True(t, exists)
	assert.Equal(t, "Original", stored.Peer.Name)
	assert.Equal(t, net.ParseIP("192.0.2.10"), stored.Peer.Address.IP)
	assert.Equal(t, 1111, stored.Peer.Address.Port)
}
