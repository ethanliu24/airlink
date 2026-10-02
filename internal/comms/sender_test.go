package comms

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const SENDER_TEST_IP = "127.0.0.1"

func toUDPAddress(ip string, port int) (*net.UDPAddr, error) {
	addr := net.JoinHostPort(ip, fmt.Sprintf("%d", port))
	return net.ResolveUDPAddr("udp", addr)
}

func mockDialErr(_ context.Context, _ net.Addr, _ *tls.Config, _ *quic.Config) (Conn, error) {
	return nil, errors.New("mock dial connection failed")
}

type mockStream struct {
	writeFunc func([]byte) (int, error)
	closeFunc func() error
}

func (s *mockStream) Write(data []byte) (int, error) {
	return s.writeFunc(data)
}

func (s *mockStream) Close() error {
	return s.closeFunc()
}

type mockConn struct {
	openStreamFunc   func() (Stream, error)
	closeWithErrFunc func(quic.ApplicationErrorCode, string) error
}

func (c *mockConn) OpenStream() (Stream, error) {
	return c.openStreamFunc()
}

func (c *mockConn) CloseWithError(
	code quic.ApplicationErrorCode,
	msg string,
) error {
	return c.closeWithErrFunc(code, msg)
}

func TestNewSender(t *testing.T) {
	t.Parallel()

	transport := &quic.Transport{}

	sender := NewSender(transport)

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
			dial:  mockDialErr,
			conns: make(map[string]Conn),
		}

		err := sender.Send(addr, []byte("test payload"))

		require.Error(t, err)
		assert.EqualError(t, err, "mock dial connection failed")
		assert.Empty(t, sender.conns)
	})

	t.Run("successful dial", func(t *testing.T) {
		addr := &net.UDPAddr{
			IP:   net.ParseIP(SENDER_TEST_IP),
			Port: 12345,
		}

		conn := &mockConn{
			openStreamFunc: func() (Stream, error) {
				return nil, errors.New("not expected")
			},
			closeWithErrFunc: func(
				quic.ApplicationErrorCode,
				string,
			) error {
				return nil
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
		}

		// sendData runs asynchronously, so the connection may be removed immediately after Send returns.
		err := sender.Send(addr, []byte("test payload"))

		require.NoError(t, err)

		require.Eventually(t, func() bool {
			sender.mu.Lock()
			defer sender.mu.Unlock()

			return len(sender.conns) == 0
		}, time.Second, time.Millisecond)
	})
}

func TestSendOverStream(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		var written []byte
		closed := false

		stream := &mockStream{
			writeFunc: func(data []byte) (int, error) {
				written = append(written, data...)
				return len(data), nil
			},
			closeFunc: func() error {
				closed = true
				return nil
			},
		}

		data := []byte("test payload")

		err := sendOverStream(stream, data)

		require.NoError(t, err)
		assert.Equal(t, data, written)
		assert.True(t, closed)
	})

	t.Run("write error", func(t *testing.T) {
		expectedErr := errors.New("write failed")
		closed := false

		stream := &mockStream{
			writeFunc: func([]byte) (int, error) {
				return 0, expectedErr
			},
			closeFunc: func() error {
				closed = true
				return nil
			},
		}

		err := sendOverStream(stream, []byte("test payload"))

		assert.ErrorIs(t, err, expectedErr)
		assert.True(t, closed)
	})

	t.Run("partial write with error", func(t *testing.T) {
		expectedErr := errors.New("partial write failed")
		closed := false

		stream := &mockStream{
			writeFunc: func(data []byte) (int, error) {
				return 5, expectedErr
			},
			closeFunc: func() error {
				closed = true
				return nil
			},
		}

		err := sendOverStream(stream, []byte("test payload"))

		assert.ErrorIs(t, err, expectedErr)
		assert.True(t, closed)
	})
}

func TestSendData(t *testing.T) {
	t.Parallel()

	t.Run("successfully opens stream and sends data", func(t *testing.T) {
		data := []byte("test payload")

		written := make(chan []byte, 1)
		streamClosed := make(chan struct{}, 1)
		connClosed := make(chan struct{}, 1)

		stream := &mockStream{
			writeFunc: func(data []byte) (int, error) {
				written <- append([]byte(nil), data...)
				return len(data), nil
			},
			closeFunc: func() error {
				streamClosed <- struct{}{}
				return nil
			},
		}

		conn := &mockConn{
			openStreamFunc: func() (Stream, error) {
				return stream, nil
			},
			closeWithErrFunc: func(
				code quic.ApplicationErrorCode,
				msg string,
			) error {
				assert.Equal(t, quic.ApplicationErrorCode(0), code)
				assert.Equal(
					t,
					"sender connection closed gracefully",
					msg,
				)

				connClosed <- struct{}{}
				return nil
			},
		}

		sender := &Sender{
			conns: map[string]Conn{
				"test": conn,
			},
		}

		sender.sendData("test", conn, data)

		select {
		case actual := <-written:
			assert.Equal(t, data, actual)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for data to be written")
		}

		select {
		case <-streamClosed:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for stream to close")
		}

		select {
		case <-connClosed:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for connection to close")
		}

		require.Eventually(t, func() bool {
			sender.mu.Lock()
			defer sender.mu.Unlock()

			_, exists := sender.conns["test"]
			return !exists
		}, time.Second, time.Millisecond)
	})

	t.Run("stream limit reached", func(t *testing.T) {
		connClosed := false

		conn := &mockConn{
			openStreamFunc: func() (Stream, error) {
				return nil, &quic.StreamLimitReachedError{}
			},
			closeWithErrFunc: func(
				quic.ApplicationErrorCode,
				string,
			) error {
				connClosed = true
				return nil
			},
		}

		sender := &Sender{
			conns: map[string]Conn{
				"test": conn,
			},
		}

		sender.sendData("test", conn, []byte("test payload"))

		assert.True(t, connClosed)

		sender.mu.Lock()
		_, exists := sender.conns["test"]
		sender.mu.Unlock()

		assert.False(t, exists)
	})

	t.Run("stream open error", func(t *testing.T) {
		expectedErr := errors.New("open stream failed")
		connClosed := false

		conn := &mockConn{
			openStreamFunc: func() (Stream, error) {
				return nil, expectedErr
			},
			closeWithErrFunc: func(
				quic.ApplicationErrorCode,
				string,
			) error {
				connClosed = true
				return nil
			},
		}

		sender := &Sender{
			conns: map[string]Conn{
				"test": conn,
			},
		}

		sender.sendData("test", conn, []byte("test payload"))

		assert.True(t, connClosed)

		sender.mu.Lock()
		_, exists := sender.conns["test"]
		sender.mu.Unlock()

		assert.False(t, exists)
	})
}

func TestCleanup(t *testing.T) {
	t.Parallel()

	t.Run("empty sender", func(t *testing.T) {
		sender := &Sender{
			conns: make(map[string]Conn),
		}

		assert.NotPanics(t, func() {
			sender.Cleanup()
		})

		assert.Empty(t, sender.conns)
	})

	t.Run("closes all connections", func(t *testing.T) {
		closed := make(map[string]bool)

		newConn := func(addr string) Conn {
			return &mockConn{
				openStreamFunc: func() (Stream, error) {
					return nil, errors.New("not expected")
				},
				closeWithErrFunc: func(
					code quic.ApplicationErrorCode,
					msg string,
				) error {
					assert.Equal(t, quic.ApplicationErrorCode(1), code)
					assert.Equal(t, "sender tearing down", msg)

					closed[addr] = true
					return nil
				},
			}
		}

		sender := &Sender{
			conns: map[string]Conn{
				"addr-1": newConn("addr-1"),
				"addr-2": newConn("addr-2"),
			},
		}

		sender.Cleanup()

		assert.True(t, closed["addr-1"])
		assert.True(t, closed["addr-2"])
		assert.Empty(t, sender.conns)
	})
}
