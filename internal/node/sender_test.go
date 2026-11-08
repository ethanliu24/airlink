package node

import (
	"airlink/internal/transfer"
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const SENDER_TEST_IP = "127.0.0.1"

var tlsConfig *tls.Config = generateTestTLSConfig()
var quicConfig *quic.Config = getTestQuicConfig()

func senderTestAddr() net.Addr {
	return &net.UDPAddr{
		IP:   net.ParseIP(SENDER_TEST_IP),
		Port: 54321,
	}
}

func TestNewSender(t *testing.T) {
	t.Parallel()

	transport := &quic.Transport{}

	send := func(transfer.TransferStream, string) (transfer.TransferId, error) {
		return "transfer-123", nil
	}

	sender := newSender(
		transport,
		senderTestAddr(),
		send,
	)

	require.NotNil(t, sender)
	require.NotNil(t, sender.dial)
	require.NotNil(t, sender.conns)
	require.NotNil(t, sender.send)

	assert.Empty(t, sender.conns)
}

func TestSend(t *testing.T) {
	t.Parallel()

	t.Run("dial error", func(t *testing.T) {
		t.Parallel()

		addr := &net.UDPAddr{
			IP:   net.ParseIP(SENDER_TEST_IP),
			Port: 12345,
		}

		var sendCalled bool

		sender := &sender{
			dial:  mockDialErr,
			conns: make(map[string]Conn),
			send: func(transfer.TransferStream, string) (transfer.TransferId, error) {
				sendCalled = true
				return "", nil
			},
		}

		err := sender.Send(addr, "test.txt", tlsConfig, quicConfig)

		require.Error(t, err)
		assert.Equal(t, "mock dial connection failed", err.Error())
		assert.Empty(t, sender.conns)
		assert.False(t, sendCalled)
	})

	t.Run("successful dial and transfer delegation", func(t *testing.T) {
		t.Parallel()

		addr := &net.UDPAddr{
			IP:   net.ParseIP(SENDER_TEST_IP),
			Port: 12345,
		}

		const (
			filename   = "test.txt"
			transferID = "transfer-123"
		)

		stream := &mockStream{}

		expectedConn := &mockConn{
			openStreamFunc: func() (transfer.TransferStream, error) {
				return stream, nil
			},
		}

		type sendCall struct {
			stream   transfer.TransferStream
			filename string
		}

		sendCalled := make(chan sendCall, 1)

		sender := &sender{
			dial: func(
				_ context.Context,
				_ net.Addr,
				_ *tls.Config,
				_ *quic.Config,
			) (Conn, error) {
				return expectedConn, nil
			},
			conns: make(map[string]Conn),
			send: func(gotStream transfer.TransferStream, gotFilename string) (transfer.TransferId, error) {
				sendCalled <- sendCall{
					stream:   gotStream,
					filename: gotFilename,
				}

				return transfer.TransferId(transferID), nil
			},
		}

		err := sender.Send(addr, filename, tlsConfig, quicConfig)
		require.NoError(t, err)

		// Wait for the asynchronous transfer delegation.
		select {
		case call := <-sendCalled:
			assert.Same(t, stream, call.stream)
			assert.Equal(t, filename, call.filename)

		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for transfer delegation")
		}

		// Verify connection registration.
		sender.mu.Lock()
		conn, exists := sender.conns[addr.String()]
		sender.mu.Unlock()

		require.True(t, exists)
		assert.Same(t, expectedConn, conn)
	})

	t.Run("open stream error", func(t *testing.T) {
		t.Parallel()

		addr := &net.UDPAddr{
			IP:   net.ParseIP(SENDER_TEST_IP),
			Port: 12345,
		}

		expectedErr := errors.New("open stream failed")
		openAttempted := make(chan struct{})

		var sendCalled bool
		var sendMu sync.Mutex

		conn := &mockConn{
			openStreamFunc: func() (transfer.TransferStream, error) {
				close(openAttempted)
				return nil, expectedErr
			},
		}

		sender := &sender{
			dial: func(
				_ context.Context,
				_ net.Addr,
				_ *tls.Config,
				_ *quic.Config,
			) (Conn, error) {
				return conn, nil
			},
			conns: make(map[string]Conn),
			send: func(transfer.TransferStream, string) (transfer.TransferId, error) {
				sendMu.Lock()
				sendCalled = true
				sendMu.Unlock()

				return "", nil
			},
		}

		err := sender.Send(addr, "test.txt", tlsConfig, quicConfig)
		require.NoError(t, err)

		select {
		case <-openAttempted:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for OpenStream")
		}

		// Verify connection registration.
		sender.mu.Lock()
		storedConn, exists := sender.conns[addr.String()]
		sender.mu.Unlock()

		require.True(t, exists)
		assert.Same(t, conn, storedConn)

		sendMu.Lock()
		assert.False(t, sendCalled)
		sendMu.Unlock()
	})
}

func TestSendOverStream(t *testing.T) {
	t.Parallel()

	t.Run("delegates to manager", func(t *testing.T) {
		t.Parallel()

		stream := &mockStream{}

		const filename = "test.txt"

		var (
			called      bool
			gotStream   transfer.TransferStream
			gotFilename string
		)

		sender := &sender{
			send: func(stream transfer.TransferStream, filename string) (transfer.TransferId, error) {
				called = true
				gotStream = stream
				gotFilename = filename

				return "transfer-123", nil
			},
		}

		err := sender.sendOverStream(stream, filename)

		require.NoError(t, err)
		assert.True(t, called)
		assert.Same(t, stream, gotStream)
		assert.Equal(t, filename, gotFilename)
	})

	t.Run("returns manager error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("manager send failed")

		sender := &sender{
			send: func(transfer.TransferStream, string) (transfer.TransferId, error) {
				return "", expectedErr
			},
		}

		err := sender.sendOverStream(&mockStream{}, "test.txt")

		require.ErrorIs(t, err, expectedErr)
	})
}

func TestCleanup(t *testing.T) {
	t.Parallel()

	var (
		mu                sync.Mutex
		closedConnections []string
	)

	conn1 := &mockConn{
		closeWithErrFunc: func(
			code quic.ApplicationErrorCode,
			msg string,
		) error {
			mu.Lock()
			closedConnections = append(closedConnections, "conn1")
			mu.Unlock()

			assert.Equal(t, quic.ApplicationErrorCode(0), code)
			assert.Equal(t, "sender connection closed gracefully", msg)

			return nil
		},
	}

	conn2 := &mockConn{
		closeWithErrFunc: func(
			code quic.ApplicationErrorCode,
			msg string,
		) error {
			mu.Lock()
			closedConnections = append(closedConnections, "conn2")
			mu.Unlock()

			assert.Equal(t, quic.ApplicationErrorCode(0), code)
			assert.Equal(t, "sender connection closed gracefully", msg)

			return nil
		},
	}

	sender := &sender{
		conns: map[string]Conn{
			"127.0.0.1:12345": conn1,
			"127.0.0.1:12346": conn2,
		},
	}

	sender.Cleanup()

	assert.Empty(t, sender.conns)

	mu.Lock()
	assert.ElementsMatch(t, []string{"conn1", "conn2"}, closedConnections)
	mu.Unlock()
}

func TestCleanupEmpty(t *testing.T) {
	t.Parallel()

	sender := &sender{
		conns: make(map[string]Conn),
	}

	assert.NotPanics(t, func() {
		sender.Cleanup()
	})

	assert.Empty(t, sender.conns)
}
