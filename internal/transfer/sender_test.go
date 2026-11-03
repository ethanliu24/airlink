
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
	"sync"
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

// newSenderTestStream creates a stream mock that safely tracks
// how many times Close() is called.
//
// The done channel is closed only once, so duplicate Close calls
// don't panic and can instead be detected with closeCount().
func newSenderTestStream(
	readFunc func([]byte) (int, error),
	writeFunc func([]byte) (int, error),
) (*mockStream, <-chan struct{}, func() int) {
	done := make(chan struct{})

	var (
		closeOnce  sync.Once
		closeMu    sync.Mutex
		closeCount int
	)

	stream := &mockStream{
		readFunc:  readFunc,
		writeFunc: writeFunc,
		closeFunc: func() error {
			closeMu.Lock()
			closeCount++
			closeMu.Unlock()

			closeOnce.Do(func() {
				close(done)
			})

			return nil
		},
	}

	getCloseCount := func() int {
		closeMu.Lock()
		defer closeMu.Unlock()

		return closeCount
	}

	return stream, done, getCloseCount
}

func assertSenderStreamClosedOnce(t *testing.T, closeCount func() int) {
	t.Helper()

	assert.Equal(t, 1, closeCount(), "stream should be closed exactly once")
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

		stream, done, closeCount := newSenderTestStream(
			responseReader.Read,
			written.Write,
		)

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
		assertSenderStreamClosedOnce(t, closeCount)

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

		stream, done, closeCount := newSenderTestStream(
			responseReader.Read,
			written.Write,
		)

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
		assertSenderStreamClosedOnce(t, closeCount)

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

	t.Run("reader open error closes stream exactly once", func(t *testing.T) {
		addr := &net.UDPAddr{
			IP:   net.ParseIP(SENDER_TEST_IP),
			Port: 12345,
		}

		expectedErr := errors.New("reader failed")

		stream, done, closeCount := newSenderTestStream(
			nil,
			func(data []byte) (int, error) {
				return len(data), nil
			},
		)

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
		assertSenderStreamClosedOnce(t, closeCount)
	})

	t.Run("stream write error closes stream exactly once", func(t *testing.T) {
		addr := &net.UDPAddr{
			IP:   net.ParseIP(SENDER_TEST_IP),
			Port: 12345,
		}

		expectedErr := errors.New("stream write failed")

		stream, done, closeCount := newSenderTestStream(
			nil,
			func([]byte) (int, error) {
				return 0, expectedErr
			},
		)

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
		assertSenderStreamClosedOnce(t, closeCount)
	})

	t.Run("response read error closes stream exactly once", func(t *testing.T) {
		addr := &net.UDPAddr{
			IP:   net.ParseIP(SENDER_TEST_IP),
			Port: 12345,
		}

		expectedErr := errors.New("response read failed")

		stream, done, closeCount := newSenderTestStream(
			func([]byte) (int, error) {
				return 0, expectedErr
			},
			func(data []byte) (int, error) {
				return len(data), nil
			},
		)

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
		assertSenderStreamClosedOnce(t, closeCount)
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

	sender := &Sender{
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

	sender := &Sender{
		conns: make(map[string]Conn),
	}

	assert.NotPanics(t, func() {
		sender.Cleanup()
	})

	assert.Empty(t, sender.conns)
}
