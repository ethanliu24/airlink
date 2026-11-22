package node

import (
	"airlink/internal/discovery"
	"airlink/internal/event"
	"airlink/internal/file"
	"airlink/internal/metadata"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const NODE_TEST_IP = "127.0.0.1"

const nodeTestTimeout = 2 * time.Second

func mockListen(network string, address *net.UDPAddr) (*net.UDPConn, error) {
	return &net.UDPConn{}, nil
}

func mockOpenReader(path string) (file.Reader, error) {
	return nil, nil
}

func toUDPAddress(ip string, port int) (*net.UDPAddr, error) {
	addr := fmt.Sprintf("%s:%d", ip, port)
	return net.ResolveUDPAddr("udp", addr)
}

type nodeTestEventPayload struct {
	Message string `json:"message"`
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

func newStartedTestNode(t *testing.T) *P2PNode {
	t.Helper()

	addr, err := toUDPAddress(NODE_TEST_IP, 0)
	require.NoError(t, err)

	node, err := newP2PNode(addr, mockListen, mockOpenReader, newNoopMDNSDiscovery)
	require.NoError(t, err)

	node.receiver.listen = func(_ *tls.Config, _ *quic.Config) (Listener, error) {
		return &mockListener{
			acceptFunc: func(context.Context) (Conn, error) {
				return nil, errors.New("listener closed")
			},
			closeFunc: func() error { return nil },
		}, nil
	}

	err = node.Start()
	require.NoError(t, err)

	t.Cleanup(node.Cleanup)

	return node
}

func TestNewP2PNodeInternal(t *testing.T) {
	discardHandler := slog.NewTextHandler(io.Discard, nil)
	logger := slog.New(discardHandler)
	slog.SetDefault(logger)

	t.Parallel()

	t.Run("success", func(t *testing.T) {
		addr, err := toUDPAddress(NODE_TEST_IP, 1234)
		require.NoError(t, err)

		node, err := newP2PNode(addr, mockListen, mockOpenReader, newNoopMDNSDiscovery)
		require.NoError(t, err)

		assert.Equal(t, fmt.Sprintf("%s:1234", NODE_TEST_IP), node.addr.String())

		t.Cleanup(func() {
			node.Cleanup()
		})
	})
}

func TestNewP2PNodeReturnsMDNSDiscoveryError(t *testing.T) {
	addr, err := toUDPAddress(NODE_TEST_IP, 1234)
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

func TestP2PNodeStart(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		node := newStartedTestNode(t)

		require.NotNil(t, node.receiver)

		err := node.Start()

		assert.Error(t, err, "starting an already-running node should fail")
	})

	t.Run("starts event listener", func(t *testing.T) {
		t.Parallel()

		node := newStartedTestNode(t)

		require.NotNil(t, node.eventManager)
	})
}

func TestP2PNodeStartBeginsDiscoveryAfterListening(t *testing.T) {
	addr, err := toUDPAddress(NODE_TEST_IP, 1234)
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
	require.NoError(t, err)

	node.receiver.listen = func(_ *tls.Config, _ *quic.Config) (Listener, error) {
		order = append(order, "listener")
		return &mockListener{
			acceptFunc: func(context.Context) (Conn, error) {
				return nil, errors.New("listener closed")
			},
			closeFunc: func() error { return nil },
		}, nil
	}

	require.NoError(t, node.Start())
	assert.Equal(t, []string{"listener", "discovery"}, order)

	node.Cleanup()
}

func TestP2PNodeStartDoesNotAdvertiseWhenListeningFails(t *testing.T) {
	addr, err := toUDPAddress(NODE_TEST_IP, 1234)
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

	node.receiver.listen = func(_ *tls.Config, _ *quic.Config) (Listener, error) {
		return nil, errors.New("listen failed")
	}

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
			IP:   net.ParseIP(NODE_TEST_IP),
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

func TestRegisterEventHandler(t *testing.T) {
	t.Parallel()

	t.Run("receives emitted event", func(t *testing.T) {
		t.Parallel()

		node := newStartedTestNode(t)

		received := make(chan event.Event, 1)

		err := node.RegisterEventHandler(func(e event.Event) error {
			received <- e
			return nil
		})

		require.NoError(t, err)

		expectedEvent, err := event.NewEvent(
			"node.test",
			&nodeTestEventPayload{
				Message: "hello",
			},
		)
		require.NoError(t, err)

		err = node.eventManager.EmitEvent(expectedEvent)
		require.NoError(t, err)

		select {
		case got := <-received:
			assert.Equal(t, expectedEvent.Name(), got.Name())

			payload, ok := got.Payload().(*nodeTestEventPayload)
			require.True(t, ok)

			assert.Equal(t, "hello", payload.Message)

		case <-time.After(nodeTestTimeout):
			t.Fatal("timed out waiting for registered event handler")
		}
	})
}

func TestNewP2PNodeGeneratesNodeID(t *testing.T) {
	addr, err := toUDPAddress(NODE_TEST_IP, 1234)
	require.NoError(t, err)

	var receivedID metadata.PeerId

	p2pNode, err := newP2PNode(
		addr,
		mockListen,
		mockOpenReader,
		func(_ *net.UDPAddr, nodeID metadata.PeerId) (discovery.Discovery, error) {
			receivedID = nodeID

			return &mockMDNSDiscovery{
				start: func() error { return nil },
				stop:  func() error { return nil },
			}, nil
		},
	)

	require.NoError(t, err)
	t.Cleanup(p2pNode.Cleanup)

	assert.NotEmpty(t, p2pNode.NodeID())
	assert.Equal(t, p2pNode.NodeID(), receivedID)
}

func TestNewP2PNodesHaveDifferentIDs(t *testing.T) {
	addr1, err := toUDPAddress(NODE_TEST_IP, 1111)
	require.NoError(t, err)

	addr2, err := toUDPAddress(NODE_TEST_IP, 2222)
	require.NoError(t, err)

	node1, err := newP2PNode(addr1, mockListen, mockOpenReader, newNoopMDNSDiscovery)
	require.NoError(t, err)
	t.Cleanup(node1.Cleanup)

	node2, err := newP2PNode(addr2, mockListen, mockOpenReader, newNoopMDNSDiscovery)
	require.NoError(t, err)
	t.Cleanup(node2.Cleanup)

	assert.NotEqual(t, node1.NodeID(), node2.NodeID())
}
