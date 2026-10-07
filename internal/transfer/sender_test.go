package comms

import (
	"airlink/internal/file"
	"context"
	"crypto/tls"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const SENDER_TEST_IP = "127.0.0.1"

var tlsConfig *tls.Config = generateTLSConfig()
var quicConfig *quic.Config = getQuicConfig()

func TestNewSender(t *testing.T) {
	t.Parallel()

	transport := &quic.Transport{}

	sender := NewSender(transport, newMockReader)

	require.NotNil(t, sender)
	require.NotNil(t, sender.dial)
	require.NotNil(t, sender.conns)
	assert.Empty(t, sender.conns)
}

func TestSend(t *testing.T) {
	t.Parallel()

	t.Run("dial error", func(t *testing.T) {
		addr := &net.UDPAddr{
			IP:   net.ParseIP(SENDER_TEST_IP),
			Port: 12345,
		}

		sender := &Sender{
			dial:       mockDialErr,
			conns:      make(map[string]Conn),
			openReader: newMockReader,
		}

		err := sender.Send(addr, "test.txt", tlsConfig, quicConfig)

		require.Error(t, err)
		assert.Equal(t, "mock dial connection failed", err.Error())
		assert.Empty(t, sender.conns)
	})

	t.Run("successful dial", func(t *testing.T) {
		addr := &net.UDPAddr{
			IP:   net.ParseIP(SENDER_TEST_IP),
			Port: 12345,
		}

		expectedConn := &mockConn{
			openStreamFunc: func() (Stream, error) {
				return &mockStream{
					writeFunc: func(data []byte) (int, error) {
						return len(data), nil
					},
					closeFunc: func() error {
						return nil
					},
				}, nil
			},
		}

		sender := &Sender{
			dial: func(
				_ context.Context,
				_ net.Addr,
				_ *tls.Config,
				_ *quic.Config,
			) (Conn, error) {
				return expectedConn, nil
			},
			conns:      make(map[string]Conn),
			openReader: newMockReader,
		}

		err := sender.Send(addr, "test.txt", tlsConfig, quicConfig)

		require.NoError(t, err)

		sender.mu.Lock()
		conn, exists := sender.conns[addr.String()]
		sender.mu.Unlock()

		require.True(t, exists)
		assert.Same(t, expectedConn, conn)
	})

	t.Run("open stream error", func(t *testing.T) {
		addr := &net.UDPAddr{
			IP:   net.ParseIP(SENDER_TEST_IP),
			Port: 12345,
		}

		expectedErr := errors.New("open stream failed")

		conn := &mockConn{
			openStreamFunc: func() (Stream, error) {
				return nil, expectedErr
			},
		}

		sender := &Sender{
			dial: func(
				_ context.Context,
				_ net.Addr,
				_ *tls.Config,
				_ *quic.Config,
			) (Conn, error) {
				return conn, nil
			},
			conns:      make(map[string]Conn),
			openReader: newMockReader,
		}

		err := sender.Send(addr, "test.txt", tlsConfig, quicConfig)

		require.NoError(t, err)

		// Send itself succeeds because the stream operation
		// happens asynchronously.
		assert.NotNil(t, sender.conns[addr.String()])
	})

	t.Run("file is written to stream", func(t *testing.T) {
		addr := &net.UDPAddr{
			IP:   net.ParseIP(SENDER_TEST_IP),
			Port: 12345,
		}

		done := make(chan struct{})
		var received []byte

		stream := &mockStream{
			writeFunc: func(data []byte) (int, error) {
				received = append(received, data...)
				return len(data), nil
			},
			closeFunc: func() error {
				close(done)
				return nil
			},
		}

		conn := &mockConn{
			openStreamFunc: func() (Stream, error) {
				return stream, nil
			},
		}

		sender := &Sender{
			dial: func(_ context.Context,
				_ net.Addr,
				_ *tls.Config,
				_ *quic.Config,
			) (Conn, error) {
				return conn, nil
			},
			conns:      make(map[string]Conn),
			openReader: newMockReader,
		}

		err := sender.Send(addr, "test.txt", tlsConfig, quicConfig)

		require.NoError(t, err)

		select {
		case <-done:
			assert.Equal(t, "test.txt", "test.txt")
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for file transfer")
		}
	})

	t.Run("reader error", func(t *testing.T) {
		addr := &net.UDPAddr{
			IP:   net.ParseIP(SENDER_TEST_IP),
			Port: 12345,
		}

		expectedErr := errors.New("reader failed")

		streamClosed := make(chan struct{})

		stream := &mockStream{
			writeFunc: func(data []byte) (int, error) {
				return len(data), nil
			},
			closeFunc: func() error {
				close(streamClosed)
				return nil
			},
		}

		conn := &mockConn{
			openStreamFunc: func() (Stream, error) {
				return stream, nil
			},
		}

		sender := &Sender{
			dial: func(
				_ context.Context,
				_ net.Addr,
				_ *tls.Config,
				_ *quic.Config,
			) (Conn, error) {
				return conn, nil
			},
			conns: make(map[string]Conn),
			openReader: func(_ string) (file.Reader, error) {
				return nil, expectedErr
			},
		}

		err := sender.Send(addr, "test.txt", tlsConfig, quicConfig)

		require.NoError(t, err)

		select {
		case <-streamClosed:
			// The stream should still be closed when
			// opening the file fails.
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for stream to close")
		}
	})

	t.Run("stream write error", func(t *testing.T) {
		addr := &net.UDPAddr{
			IP:   net.ParseIP(SENDER_TEST_IP),
			Port: 12345,
		}

		expectedErr := errors.New("stream write failed")
		streamClosed := make(chan struct{})

		stream := &mockStream{
			writeFunc: func(data []byte) (int, error) {
				return 0, expectedErr
			},
			closeFunc: func() error {
				close(streamClosed)
				return nil
			},
		}

		conn := &mockConn{
			openStreamFunc: func() (Stream, error) {
				return stream, nil
			},
		}

		sender := &Sender{
			dial: func(
				_ context.Context,
				_ net.Addr,
				_ *tls.Config,
				_ *quic.Config,
			) (Conn, error) {
				return conn, nil
			},
			conns:      make(map[string]Conn),
			openReader: newMockReader,
		}

		err := sender.Send(addr, "test.txt", tlsConfig, quicConfig)

		require.NoError(t, err)

		select {
		case <-streamClosed:
			// Expected.
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for stream to close")
		}
	})
}

func TestCleanup(t *testing.T) {
	t.Parallel()

	var closedConnections []string

	conn1 := &mockConn{
		closeWithErrFunc: func(
			code quic.ApplicationErrorCode,
			msg string,
		) error {
			closedConnections = append(closedConnections, "conn1")

			assert.Equal(t, quic.ApplicationErrorCode(0x0), code)
			assert.Equal(t, "sender connection closed gracefully", msg)

			return nil
		},
	}

	conn2 := &mockConn{
		closeWithErrFunc: func(
			code quic.ApplicationErrorCode,
			msg string,
		) error {
			closedConnections = append(closedConnections, "conn2")

			assert.Equal(t, quic.ApplicationErrorCode(0x0), code)
			assert.Equal(t, "sender connection closed gracefully", msg)

			return nil
		},
	}

	sender := &Sender{
		conns: map[string]Conn{
			"127.0.0.1:12345": conn1,
			"127.0.0.1:12346": conn2,
		},
	}

	sender.Cleanup()

	assert.Empty(t, sender.conns)
	assert.ElementsMatch(
		t,
		[]string{"conn1", "conn2"},
		closedConnections,
	)
}

func TestCleanupEmpty(t *testing.T) {
	t.Parallel()

	sender := &Sender{
		conns: make(map[string]Conn),
	}

	assert.NotPanics(t, func() {
		sender.Cleanup()
	})

	assert.Empty(t, sender.conns)
}
