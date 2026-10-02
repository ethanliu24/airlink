package comms

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"

	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const SENDER_TEST_IP = "127.0.0.1"

func toUDPAddress(ip string, port int) (*net.UDPAddr, error) {
	addr := fmt.Sprintf("%s:%d", ip, port)
	return net.ResolveUDPAddr("udp", addr)
}

func mockDialErr(_ context.Context, _ net.Addr, _ *tls.Config, _ *quic.Config) (*quic.Conn, error) {
	return nil, errors.New("mock dial connection failed")
}

func TestNewSender(t *testing.T) {
	discardHandler := slog.NewTextHandler(io.Discard, nil)
	logger := slog.New(discardHandler)
	slog.SetDefault(logger)

	t.Parallel()

	t.Run("success initialization", func(t *testing.T) {
		transport := &quic.Transport{}

		sender := NewSender(transport)
		require.NotNil(t, sender)
		assert.Equal(t, transport, sender.transport)

		t.Cleanup(func() {
			sender.Cleanup()
		})
	})
}

func TestSend(t *testing.T) {
	discardHandler := slog.NewTextHandler(io.Discard, nil)
	logger := slog.New(discardHandler)
	slog.SetDefault(logger)

	t.Parallel()

	t.Run("failure", func(t *testing.T) {
		t.Run("dial error with uninitialized transport", func(t *testing.T) {
			addr, err := toUDPAddress(SENDER_TEST_IP, 0)
			require.NoError(t, err)

			sender := NewSender(&quic.Transport{})
			sender.dial = mockDialErr
			data := []byte("test payload")

			err = sender.Send(addr, data)
			assert.Error(t, err)
		})
	})
}
