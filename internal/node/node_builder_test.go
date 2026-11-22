package node

import (
	"airlink/internal/file"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	nodeTestIP      = "127.0.0.1"
	nodeTestTimeout = 2 * time.Second
)

func mockOpenReader(path string) (file.Reader, error) {
	return nil, nil
}

const (
	TEST_VALID_IP   = "127.0.0.1"
	TEST_IPV6       = "2001:db8::1"
	TEST_INVALID_IP = "999.999.999.999"
)

func TestCreateUDPAddress(t *testing.T) {
	t.Parallel()

	t.Run("success with ipv4", func(t *testing.T) {
		port := 8080
		addr, err := CreateUDPAddress(TEST_VALID_IP, port)

		require.NoError(t, err)
		require.NotNil(t, addr)
		assert.Equal(t, TEST_VALID_IP, addr.IP.String())
		assert.Equal(t, port, addr.Port)
	})

	t.Run("success with ipv6", func(t *testing.T) {
		port := 9000
		addr, err := CreateUDPAddress(TEST_IPV6, port)

		require.NoError(t, err)
		require.NotNil(t, addr)
		assert.Equal(t, TEST_IPV6, addr.IP.String())
		assert.Equal(t, port, addr.Port)
	})

	t.Run("success with port zero for dynamic allocation", func(t *testing.T) {
		port := 0
		addr, err := CreateUDPAddress(TEST_VALID_IP, port)

		require.NoError(t, err)
		require.NotNil(t, addr)
		assert.Equal(t, TEST_VALID_IP, addr.IP.String())
		assert.Equal(t, port, addr.Port) // The address structure will hold 0 until bound
	})

	t.Run("success with empty ip defaults to wildcard listener", func(t *testing.T) {
		port := 53
		addr, err := CreateUDPAddress("", port)

		require.NoError(t, err)
		require.NotNil(t, addr)
		assert.Nil(t, addr.IP) // Go resolves an empty string host to a nil IP field
		assert.Equal(t, port, addr.Port)
	})

	t.Run("error with invalid ip format", func(t *testing.T) {
		addr, err := CreateUDPAddress(TEST_INVALID_IP, 1234)

		assert.Error(t, err)
		assert.Nil(t, addr)
	})

	t.Run("error with negative port range", func(t *testing.T) {
		addr, err := CreateUDPAddress(TEST_VALID_IP, -1)

		assert.Error(t, err)
		assert.Nil(t, addr)
	})

	t.Run("error with port range overflow", func(t *testing.T) {
		addr, err := CreateUDPAddress(TEST_VALID_IP, 65536)

		assert.Error(t, err)
		assert.Nil(t, addr)
	})
}

func newStartedTestNode(t *testing.T) *P2PNode {
	t.Helper()

	addr, err := CreateUDPAddress(nodeTestIP, 0)
	require.NoError(t, err)

	node, err := newP2PNode(addr, mockListen, mockOpenReader, newNoopMDNSDiscovery)
	require.NoError(t, err)

	node.receiver = &mockReceiver{
		listenFunc:  func(tlsCfg *tls.Config, quicCfg *quic.Config) error { return nil },
		cleanUpFunc: func() {},
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
		addr, err := CreateUDPAddress(nodeTestIP, 1234)
		require.NoError(t, err)

		node, err := newP2PNode(addr, mockListen, mockOpenReader, newNoopMDNSDiscovery)
		require.NoError(t, err)

		assert.Equal(t, fmt.Sprintf("%s:1234", nodeTestIP), node.addr.String())

		t.Cleanup(func() {
			node.Cleanup()
		})
	})
}
