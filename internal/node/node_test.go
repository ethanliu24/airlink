
package node

import (
	"airlink/internal/event"
	"airlink/internal/file"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

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

func newStartedTestNode(t *testing.T) *P2PNode {
	t.Helper()

	addr, err := toUDPAddress(NODE_TEST_IP, 0)
	require.NoError(t, err)

	node, err := newP2PNode(addr, net.ListenUDP, mockOpenReader)
	require.NoError(t, err)

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

		node, err := newP2PNode(addr, mockListen, mockOpenReader)
		require.NoError(t, err)

		assert.Equal(t, fmt.Sprintf("%s:1234", NODE_TEST_IP), node.addr.String())

		t.Cleanup(func() {
			node.Cleanup()
		})
	})
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
