package node

import (
	"airlink/internal/discovery"
	"airlink/internal/event"
	"airlink/internal/file"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
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

func newNoopMDNSDiscovery(_ *net.UDPAddr) (discovery.Discovery, error) {
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
		func(*net.UDPAddr) (discovery.Discovery, error) { return nil, wantErr },
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
		func(_ *net.UDPAddr) (discovery.Discovery, error) { return mdns, nil },
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

	node.receiver.Cleanup()
	node.eventManager.CleanUp()
	node.transferManager.CleanUp()
}

func TestP2PNodeStartDoesNotAdvertiseWhenListeningFails(t *testing.T) {
	addr, err := toUDPAddress(NODE_TEST_IP, 1234)
	require.NoError(t, err)

	started := false
	node, err := newP2PNode(
		addr,
		mockListen,
		mockOpenReader,
		func(_ *net.UDPAddr) (discovery.Discovery, error) {
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

    expected := []discovery.Peer{
        {
            Name: "Test",
            Address: &net.UDPAddr{
                IP:   net.ParseIP("192.0.2.20"),
                Port: 1234,
            },
        },
    }

    node, err := newP2PNode(
        &net.UDPAddr{
            IP:   net.ParseIP(NODE_TEST_IP),
            Port: 1234,
        },
        mockListen,
        mockOpenReader,
        func(_ *net.UDPAddr) (discovery.Discovery, error) {
            return &mockMDNSDiscovery{
                start: func() error { return nil },
                stop:  func() error { return nil },
                discover: func() ([]discovery.Peer, error) {
                    return expected, nil
                },
            }, nil
        },
    )

    require.NoError(t, err)

    peers, err := node.DiscoverPeers()

    require.NoError(t, err)
    require.Equal(t, expected, peers)

    t.Cleanup(node.Cleanup)
}

func TestP2PNodeDiscoverPeersFiltersSelfAndForwardsOtherPeers(t *testing.T) {
    localAddr := &net.UDPAddr{
        IP:   net.ParseIP("192.0.2.10"),
        Port: 1111,
    }

    discoveredPeers := []discovery.Peer{
        {
            Name: "self",
            Address: &net.UDPAddr{
                IP:   localAddr.IP,
                Port: localAddr.Port,
            },
        },
        {
            Name: "different-ip",
            Address: &net.UDPAddr{
                IP:   net.ParseIP("192.0.2.20"),
                Port: 1111,
            },
        },
        {
            Name: "different-port",
            Address: &net.UDPAddr{
                IP:   localAddr.IP,
                Port: 2222,
            },
        },
        {
            Name: "ipv6-peer",
            Address: &net.UDPAddr{
                IP:   net.ParseIP("2001:db8::2"),
                Port: 1111,
            },
        },
        {
            Name: "no-address",
        },
    }

    p2pNode := &P2PNode{
        addr: localAddr,
        mdnsDiscovery: &mockMDNSDiscovery{
            discover: func() ([]discovery.Peer, error) {
                return discoveredPeers, nil
            },
        },
    }

    peers, err := p2pNode.DiscoverPeers()

    require.NoError(t, err)
    require.Len(t, peers, 4)

    assert.Equal(t, "different-ip", peers[0].Name)
    assert.Equal(t, "different-port", peers[1].Name)
    assert.Equal(t, "ipv6-peer", peers[2].Name)
    assert.Equal(t, "no-address", peers[3].Name)
}

func TestP2PNodeDiscoverPeersReturnsDiscoveryError(t *testing.T) {
    discoveryErr := errors.New("discovery failed")

    expected := []discovery.Peer{
        {
            Name: "peer",
            Address: &net.UDPAddr{
                IP:   net.ParseIP("192.0.2.20"),
                Port: 1234,
            },
        },
    }

    p2pNode := &P2PNode{
        addr: testAddr(),
        mdnsDiscovery: &mockMDNSDiscovery{
            discover: func() ([]discovery.Peer, error) {
                return expected, discoveryErr
            },
        },
    }

    peers, err := p2pNode.DiscoverPeers()

    require.ErrorIs(t, err, discoveryErr)
    assert.Equal(t, expected, peers)
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
