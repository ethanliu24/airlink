package transfer

import (
	"airlink/internal/file"
	"airlink/internal/transfer/models"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
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

func senderTestAddr() net.Addr {
	return &net.UDPAddr{
		IP:   net.ParseIP(SENDER_TEST_IP),
		Port: 54321,
	}
}

func newSenderAcceptedResponseReader(t *testing.T) *bytes.Reader {
	t.Helper()

	var buf bytes.Buffer

	err := writeMessage(&buf, &models.TransferResponse{
		Accepted: true,
	})
	require.NoError(t, err)

	return bytes.NewReader(buf.Bytes())
}

func waitForSenderStreamClose(t *testing.T, done <-chan struct{}) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for transfer stream to close")
	}
}

func TestNewSender(t *testing.T) {
	t.Parallel()

	transport := &quic.Transport{}

	sender := NewSender(transport, newMockReader, senderTestAddr())

	require.NotNil(t, sender)
	require.NotNil(t, sender.dial)
	require.NotNil(t, sender.conns)
	assert.Empty(t, sender.conns)
	assert.Equal(t, senderTestAddr(), sender.addr)
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
			addr:       senderTestAddr(),
		}

		err := sender.Send(addr, "test.txt", tlsConfig, quicConfig)

		require.Error(t, err)
		assert.Equal(t, "mock dial connection failed", err.Error())
		assert.Empty(t, sender.conns)
	})

	t.Run("successful dial and transfer", func(t *testing.T) {
		addr := &net.UDPAddr{
			IP:   net.ParseIP(SENDER_TEST_IP),
			Port: 12345,
		}

		responseReader := newSenderAcceptedResponseReader(t)
		var written bytes.Buffer
		done := make(chan struct{})

		stream := &mockStream{
			readFunc: responseReader.Read,
			writeFunc: written.Write,
			closeFunc: func() error {
				close(done)
				return nil
			},
		}

		expectedConn := &mockConn{
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
				return expectedConn, nil
			},
			conns: make(map[string]Conn),
			openReader: func(_ string) (file.Reader, error) {
				return newMockReader("hello world")
			},
			addr: senderTestAddr(),
		}

		err := sender.Send(addr, "test.txt", tlsConfig, quicConfig)
		require.NoError(t, err)

		waitForSenderStreamClose(t, done)

		sender.mu.Lock()
		conn, exists := sender.conns[addr.String()]
		sender.mu.Unlock()

		require.True(t, exists)
		assert.Same(t, expectedConn, conn)
		assert.NotZero(t, written.Len())
	})

	t.Run("open stream error", func(t *testing.T) {
		addr := &net.UDPAddr{
			IP:   net.ParseIP(SENDER_TEST_IP),
			Port: 12345,
		}

		expectedErr := errors.New("open stream failed")
		openAttempted := make(chan struct{})

		conn := &mockConn{
			openStreamFunc: func() (Stream, error) {
				close(openAttempted)
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
			addr:       senderTestAddr(),
		}

		err := sender.Send(addr, "test.txt", tlsConfig, quicConfig)
		require.NoError(t, err)

		select {
		case <-openAttempted:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for OpenStream")
		}

		sender.mu.Lock()
		storedConn, exists := sender.conns[addr.String()]
		sender.mu.Unlock()

		require.True(t, exists)
		assert.Same(t, conn, storedConn)
	})

	t.Run("writes request and file data to stream", func(t *testing.T) {
		addr := &net.UDPAddr{
			IP:   net.ParseIP(SENDER_TEST_IP),
			Port: 12345,
		}

		responseReader := newSenderAcceptedResponseReader(t)
		var written bytes.Buffer
		done := make(chan struct{})

		stream := &mockStream{
			readFunc: responseReader.Read,
			writeFunc: written.Write,
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
				return newMockReader("hello world")
			},
			addr: senderTestAddr(),
		}

		err := sender.Send(addr, "test.txt", tlsConfig, quicConfig)
		require.NoError(t, err)

		waitForSenderStreamClose(t, done)

		out := bytes.NewReader(written.Bytes())
		request := &models.TransferRequest{}

		require.NoError(t, readMessage(out, request))

		assert.Equal(t, "report.txt", request.GetFilename())
		assert.Equal(t, uint64(len("hello world")), request.GetFileSize())
		assert.NotEmpty(t, request.GetMime())

		fileData, err := io.ReadAll(out)
		require.NoError(t, err)
		assert.Equal(t, "hello world", string(fileData))
	})

	t.Run("reader open error closes stream", func(t *testing.T) {
		addr := &net.UDPAddr{
			IP:   net.ParseIP(SENDER_TEST_IP),
			Port: 12345,
		}

		expectedErr := errors.New("reader failed")
		done := make(chan struct{})

		stream := &mockStream{
			writeFunc: func(data []byte) (int, error) {
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
			addr: senderTestAddr(),
		}

		err := sender.Send(addr, "test.txt", tlsConfig, quicConfig)
		require.NoError(t, err)

		waitForSenderStreamClose(t, done)
	})

	t.Run("stream write error closes stream", func(t *testing.T) {
		addr := &net.UDPAddr{
			IP:   net.ParseIP(SENDER_TEST_IP),
			Port: 12345,
		}

		expectedErr := errors.New("stream write failed")
		done := make(chan struct{})

		stream := &mockStream{
			writeFunc: func([]byte) (int, error) {
				return 0, expectedErr
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
			addr:       senderTestAddr(),
		}

		err := sender.Send(addr, "test.txt", tlsConfig, quicConfig)
		require.NoError(t, err)

		waitForSenderStreamClose(t, done)
	})

	t.Run("response read error closes stream", func(t *testing.T) {
		addr := &net.UDPAddr{
			IP:   net.ParseIP(SENDER_TEST_IP),
			Port: 12345,
		}

		expectedErr := errors.New("response read failed")
		done := make(chan struct{})

		stream := &mockStream{
			writeFunc: func(data []byte) (int, error) {
				return len(data), nil
			},
			readFunc: func([]byte) (int, error) {
				return 0, expectedErr
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
				return newMockReader("hello world")
			},
			addr: senderTestAddr(),
		}

		err := sender.Send(addr, "test.txt", tlsConfig, quicConfig)
		require.NoError(t, err)

		waitForSenderStreamClose(t, done)
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
			closedConnections = append(closedConnections, "conn2")

			assert.Equal(t, quic.ApplicationErrorCode(0), code)
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
	assert.ElementsMatch(t, []string{"conn1", "conn2"}, closedConnections)
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
