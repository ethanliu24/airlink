package node

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const NODE_TEST_IP = "127.0.0.1"

func mockListen(network string, address *net.UDPAddr) (*net.UDPConn, error) {
	return &net.UDPConn{}, nil
}

func toUDPAddress(ip string, port int) (*net.UDPAddr, error) {
	addr := fmt.Sprintf("%s:%d", ip, port)
	return net.ResolveUDPAddr("udp", addr)
}

func TestNewP2PNode(t *testing.T) {
	discardHandler := slog.NewTextHandler(io.Discard, nil)
	logger := slog.New(discardHandler)
	slog.SetDefault(logger)
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		addr, err := toUDPAddress(NODE_TEST_IP, 1234)
		require.NoError(t, err)

		node, err := NewP2PNode(addr, mockListen)
		require.NoError(t, err)
		require.NotNil(t, node.transport)
		assert.Equal(t, fmt.Sprintf("%s:1234", NODE_TEST_IP), node.addr.String())

		t.Cleanup(func() {
			node.Cleanup()
		})
	})
}
