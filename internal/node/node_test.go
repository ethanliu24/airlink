package node

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewP2PNode(t *testing.T) {
	discardHandler := slog.NewTextHandler(io.Discard, nil)
	logger := slog.New(discardHandler)
	slog.SetDefault(logger)
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		node, err := NewP2PNode(0)

		require.NoError(t, err)
		require.NotNil(t, node.transport)

		t.Cleanup(func() {
			node.Cleanup()
		})
	})

	t.Run("failure", func(t *testing.T) {
		t.Parallel()

		t.Run("invalid port number", func(t *testing.T) {
			node, err := NewP2PNode(999999)

			require.Error(t, err)
			require.Nil(t, node)
		})

		t.Run("negative port number", func(t *testing.T) {
			node, err := NewP2PNode(-1)

			require.Error(t, err)
			require.Nil(t, node)
		})

		t.Run("port already in use", func(t *testing.T) {
			port := 1234
			node, err := NewP2PNode(port)
			require.NoError(t, err)
			require.NotNil(t, node)

			nodeOther, err := NewP2PNode(port)
			require.Error(t, err)
			require.Nil(t, nodeOther)

			t.Cleanup(func () {
				node.Cleanup()
			})
		})
	})
}
