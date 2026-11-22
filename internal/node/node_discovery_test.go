package node

import (
	"airlink/internal/discovery"
	"airlink/internal/metadata"
	"crypto/tls"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockMDNSDiscovery struct {
	start    func() error
	stop     func() error
	discover func() ([]discovery.Peer, error)
}

func (d *mockMDNSDiscovery) Start() error {
	return d.start()
}

func (d *mockMDNSDiscovery) Stop() error {
	return d.stop()
}

func (d *mockMDNSDiscovery) Discover() ([]discovery.Peer, error) {
	if d.discover == nil {
		return nil, nil
	}

	return d.discover()
}

func newNoopMDNSDiscovery(
	_ *net.UDPAddr,
	_ metadata.PeerId,
) (discovery.Discovery, error) {
	return &mockMDNSDiscovery{
		start: func() error { return nil },
		stop:  func() error { return nil },
	}, nil
}

func TestNewP2PNodeReturnsMDNSDiscoveryError(t *testing.T) {
	addr, err := CreateUDPAddress(nodeTestIP, 1234)
	require.NoError(t, err)

	wantErr := errors.New("mDNS discovery creation failed")
	node, err := newP2PNode(
		addr,
		mockListen,
		mockOpenReader,
		func(addr *net.UDPAddr, nodeID metadata.PeerId) (discovery.Discovery, error) {
			return nil, wantErr
		},
	)

	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, node)
}

func TestP2PNodeStartBeginsDiscoveryAfterListening(t *testing.T) {
	addr, err := CreateUDPAddress(nodeTestIP, 1234)
	require.NoError(t, err)

	order := make([]string, 0, 2)

	mdns := &mockMDNSDiscovery{
		start: func() error {
			order = append(order, "discovery")
			return nil
		},
		stop: func() error { return nil },
	}

	node, err := newP2PNode(
		addr,
		mockListen,
		mockOpenReader,
		func(addr *net.UDPAddr, nodeID metadata.PeerId) (discovery.Discovery, error) {
			return mdns, nil
		},
	)
	node.receiver = &mockReceiver{
		listenFunc: func(tlsCfg *tls.Config, quicCfg *quic.Config) error {
			order = append(order, "listener")
			return nil
		},
		cleanUpFunc: func() {},
	}

	require.NoError(t, err)

	require.NoError(t, node.Start())
	assert.Equal(t, []string{"listener", "discovery"}, order)

	node.Cleanup()
}

func TestP2PNodeStartDoesNotAdvertiseWhenListeningFails(t *testing.T) {
	addr, err := CreateUDPAddress(nodeTestIP, 1234)
	require.NoError(t, err)

	started := false
	node, err := newP2PNode(
		addr,
		mockListen,
		mockOpenReader,
		func(addr *net.UDPAddr, nodeID metadata.PeerId) (discovery.Discovery, error) {
			return &mockMDNSDiscovery{
				start: func() error {
					started = true
					return nil
				},
				stop: func() error { return nil },
			}, nil
		},
	)
	require.NoError(t, err)

	require.Error(t, node.Start())
	assert.False(t, started)

	node.eventManager.CleanUp()
	node.transferManager.CleanUp()
}

func TestP2PNodePeerDiscovery(t *testing.T) {
	t.Parallel()

	expected := discovery.Peer{
		Id:   metadata.PeerId("peer-1"),
		Name: "Test",
		Address: &net.UDPAddr{
			IP:   net.ParseIP("192.0.2.20"),
			Port: 1234,
		},
	}

	node, err := newP2PNode(
		&net.UDPAddr{
			IP:   net.ParseIP(nodeTestIP),
			Port: 1234,
		},
		mockListen,
		mockOpenReader,
		func(addr *net.UDPAddr, nodeID metadata.PeerId) (discovery.Discovery, error) {
			return &mockMDNSDiscovery{
				start: func() error { return nil },
				stop:  func() error { return nil },
				discover: func() ([]discovery.Peer, error) {
					return []discovery.Peer{expected}, nil
				},
			}, nil
		},
	)

	require.NoError(t, err)

	node.peerManager.Discover()
	records := node.GetPeers()

	require.NoError(t, err)
	require.Len(t, records, 1)

	assert.Equal(t, expected, records[0].Peer)
	assert.False(t, records[0].FirstSeenAt.IsZero())
	assert.Equal(t, records[0].FirstSeenAt, records[0].LastSeenAt)

	t.Cleanup(node.Cleanup)
}

func TestP2PNodeStartPeerDiscovery(t *testing.T) {
	var discoverCalls atomic.Int32

	mdns := &mockMDNSDiscovery{
		discover: func() ([]discovery.Peer, error) {
			discoverCalls.Add(1)
			return nil, nil
		},
	}

	peerManager, err := discovery.NewPeerManager(
		metadata.PeerId("local-node"),
		mdns,
	)
	require.NoError(t, err)

	node := &P2PNode{
		peerManager: peerManager,
	}

	// Start with a long interval so no discovery happens yet.
	node.StartPeerDiscovery(time.Hour)

	// Starting again should replace the previous polling loop.
	restarted := make(chan struct{})

	go func() {
		node.StartPeerDiscovery(10 * time.Millisecond)
		close(restarted)
	}()

	select {
	case <-restarted:
	case <-time.After(nodeTestTimeout):
		t.Fatal("restarting peer discovery timed out")
	}

	t.Cleanup(node.StopPeerDiscovery)

	// Verify the new polling loop performs discovery periodically.
	require.Eventually(t, func() bool {
		return discoverCalls.Load() >= 2
	}, nodeTestTimeout, 5*time.Millisecond)
}

func TestP2PNodeStopPeerDiscovery(t *testing.T) {
	var discoverCalls atomic.Int32

	mdns := &mockMDNSDiscovery{
		discover: func() ([]discovery.Peer, error) {
			discoverCalls.Add(1)
			return nil, nil
		},
	}

	peerManager, err := discovery.NewPeerManager(
		metadata.PeerId("local-node"),
		mdns,
	)
	require.NoError(t, err)

	node := &P2PNode{
		peerManager: peerManager,
	}

	pollingFrequency := 100 * time.Millisecond

	node.StartPeerDiscovery(pollingFrequency)
	t.Cleanup(node.StopPeerDiscovery)

	// Wait until the first discovery happens.
	require.Eventually(t, func() bool {
		return discoverCalls.Load() >= 1
	}, nodeTestTimeout, 5*time.Millisecond)

	node.StopPeerDiscovery()

	callsAfterStop := discoverCalls.Load()

	// Wait several polling intervals and verify no more discoveries occur.
	time.Sleep(3 * pollingFrequency)

	assert.Equal(t, callsAfterStop, discoverCalls.Load())

	// Stopping again should be safe.
	node.StopPeerDiscovery()
}
