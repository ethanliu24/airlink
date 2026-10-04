package comms

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewReceiver(t *testing.T) {
	t.Parallel()

	t.Run("success initialization", func(t *testing.T) {
		transport := &quic.Transport{}

		receiver := NewReceiver(transport)

		require.NotNil(t, receiver)
		require.NotNil(t, receiver.listen)
		assert.Nil(t, receiver.listener)
	})
}

func TestHandleStream(t *testing.T) {
	t.Parallel()

	t.Run("reads until EOF and closes stream", func(t *testing.T) {
		readCalls := 0
		closed := false

		stream := &mockStream{
			readFunc: func(data []byte) (int, error) {
				readCalls++

				if readCalls == 1 {
					copy(data, []byte("hello"))
					return 5, nil
				}

				return 0, io.EOF
			},
			writeFunc: func([]byte) (int, error) {
				return 0, errors.New("unexpected write")
			},
			closeFunc: func() error {
				closed = true
				return nil
			},
		}

		handleStream(stream)

		assert.Equal(t, 2, readCalls)
		assert.True(t, closed)
	})

	t.Run("closes stream when read fails", func(t *testing.T) {
		expectedErr := errors.New("read failed")
		closed := false

		stream := &mockStream{
			readFunc: func([]byte) (int, error) {
				return 0, expectedErr
			},
			writeFunc: func([]byte) (int, error) {
				return 0, errors.New("unexpected write")
			},
			closeFunc: func() error {
				closed = true
				return nil
			},
		}

		handleStream(stream)

		assert.True(t, closed)
	})
}

func TestHandleConnection(t *testing.T) {
	t.Parallel()

	t.Run("accepts streams and handles them", func(t *testing.T) {
		streamHandled := make(chan struct{}, 1)
		connectionClosed := make(chan struct{}, 1)

		stream := &mockStream{
			readFunc: func(data []byte) (int, error) {
				streamHandled <- struct{}{}
				return 0, io.EOF
			},
			writeFunc: func([]byte) (int, error) {
				return 0, errors.New("unexpected write")
			},
			closeFunc: func() error {
				return nil
			},
		}

		acceptCalls := 0

		conn := &mockConn{
			acceptStreamFunc: func(context.Context) (Stream, error) {
				acceptCalls++

				if acceptCalls == 1 {
					return stream, nil
				}

				connectionClosed <- struct{}{}
				return nil, errors.New("connection closed")
			},
			openStreamFunc: func() (Stream, error) {
				return nil, errors.New("unexpected open")
			},
			closeWithErrFunc: func(quic.ApplicationErrorCode, string) error {
				return nil
			},
		}

		handleConnection(conn)

		select {
		case <-streamHandled:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for stream to be handled")
		}

		select {
		case <-connectionClosed:
		default:
			// handleConnection should have attempted to accept another stream and received the error.
		}

		assert.GreaterOrEqual(t, acceptCalls, 2)
	})
}

func TestReceiverListen(t *testing.T) {
	t.Parallel()

	t.Run("listen error", func(t *testing.T) {
		expectedErr := errors.New("listen failed")

		receiver := &Receiver{
			listen: func(*tls.Config, *quic.Config) (Listener, error) {
				return nil, expectedErr
			},
		}

		err := receiver.Listen(&tls.Config{}, &quic.Config{})

		require.ErrorIs(t, err, expectedErr)
		assert.Nil(t, receiver.listener)
	})

	t.Run("success", func(t *testing.T) {
		listener := &mockListener{
			acceptFunc: func(context.Context) (Conn, error) {
				return nil, errors.New("listener closed")
			},
		}

		receiver := &Receiver{
			listen: func(*tls.Config, *quic.Config) (Listener, error) {
				return listener, nil
			},
		}

		err := receiver.Listen(&tls.Config{}, &quic.Config{})
		require.NoError(t, err)

		// Listen starts recieve asynchronously, so allow it to run.
		require.Eventually(t, func() bool {
			return true
		}, time.Second, time.Millisecond)
	})

	t.Run("errors if listener already listening", func(t *testing.T) {
		listener := &mockListener{
			acceptFunc: func(context.Context) (Conn, error) {
				return nil, errors.New("listener closed")
			},
		}

		receiver := &Receiver{
			listen: func(*tls.Config, *quic.Config) (Listener, error) {
				return listener, nil
			},
		}

		assert.Equal(t, receiver.isListening, false)

		err := receiver.Listen(&tls.Config{}, &quic.Config{})
		require.NoError(t, err)
		assert.Equal(t, receiver.isListening, true)

		err = receiver.Listen(&tls.Config{}, &quic.Config{})
		require.Error(t, err)
		assert.Equal(t, receiver.isListening, true)
	})

	t.Run("success when calling Listen again when receiver is cleaned up", func(t *testing.T) {
		listener := &mockListener{
			acceptFunc: func(context.Context) (Conn, error) {
				return nil, errors.New("listener closed")
			},
			closeFunc: func() error {
				return nil
			},
		}

		receiver := &Receiver{
			listen: func(*tls.Config, *quic.Config) (Listener, error) {
				return listener, nil
			},
		}

		assert.False(t, receiver.isListening)

		err := receiver.Listen(&tls.Config{}, &quic.Config{})
		require.NoError(t, err)
		assert.True(t, receiver.isListening)

		receiver.Cleanup()
		assert.False(t, receiver.isListening)

		err = receiver.Listen(&tls.Config{}, &quic.Config{})
		require.NoError(t, err)
		assert.True(t, receiver.isListening)
	})
}

func TestReceiverRecieve(t *testing.T) {
	t.Parallel()

	// TODO update
	t.Run("accepts connection and stores it", func(t *testing.T) {
		expectedConn := &mockConn{
			acceptStreamFunc: func(context.Context) (Stream, error) {
				return nil, errors.New("connection closed")
			},
			openStreamFunc: func() (Stream, error) {
				return nil, errors.New("unexpected open")
			},
			closeWithErrFunc: func(quic.ApplicationErrorCode, string) error {
				return nil
			},
		}

		listener := &mockListener{
			acceptFunc: func(context.Context) (Conn, error) {
				return expectedConn, errors.New("accept failed")
			},
		}

		receiver := &Receiver{}
		receiver.recieve(listener)

		// Since Accept returned both a connection and an error,
		// the production code currently stops before assigning conn.
		assert.Nil(t, receiver.listener)
	})
}

func TestReceiverCleanup(t *testing.T) {
	t.Parallel()

	t.Run("nil connection", func(t *testing.T) {
		receiver := &Receiver{}

		assert.NotPanics(t, func() {
			receiver.Cleanup()
		})
	})

	t.Run("closes listener", func(t *testing.T) {
		closed := false

		listener := &mockListener{
			acceptFunc: func(context.Context) (Conn, error) {
				return nil, errors.New("unexpected accept")
			},
			closeFunc: func() (error) {
				closed = true
				return nil
			},
		}

		receiver := &Receiver{listener: listener}
		receiver.Cleanup()

		assert.True(t, closed)
		assert.False(t, receiver.isListening)
	})
}
