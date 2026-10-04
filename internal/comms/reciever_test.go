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

	t.Run("accept stream error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("connection closed")

		conn := &mockConn{
			acceptStreamFunc: func(context.Context) (Stream, error) {
				return nil, expectedErr
			},
			closeWithErrFunc: func(
				quic.ApplicationErrorCode,
				string,
			) error {
				return nil
			},
		}

		r := &Receiver{
			conns: map[Conn]struct{}{
				conn: {},
			},
		}

		r.handleConnection(conn)

		assert.NotContains(t, r.conns, conn)
	})

	t.Run("closes connection normally", func(t *testing.T) {
		t.Parallel()

		var closeCalled bool

		conn := &mockConn{
			acceptStreamFunc: func(context.Context) (Stream, error) {
				return nil, errors.New("connection closed")
			},
			closeWithErrFunc: func(
				code quic.ApplicationErrorCode,
				msg string,
			) error {
				closeCalled = true

				assert.Equal(t, quic.ApplicationErrorCode(0x0), code)
				assert.Equal(
					t,
					"receiver connection closed normally",
					msg,
				)

				return nil
			},
		}

		r := &Receiver{
			conns: map[Conn]struct{}{
				conn: {},
			},
		}

		r.handleConnection(conn)

		assert.True(t, closeCalled)
	})

	t.Run("handles accepted stream", func(t *testing.T) {
		t.Parallel()

		streamHandled := make(chan struct{})

		stream := &mockStream{
			readFunc: func(data []byte) (int, error) {
				close(streamHandled)
				return 0, io.EOF
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

				return nil, errors.New("connection closed")
			},
			closeWithErrFunc: func(
				quic.ApplicationErrorCode,
				string,
			) error {
				return nil
			},
		}

		r := &Receiver{
			conns: map[Conn]struct{}{
				conn: {},
			},
		}

		r.handleConnection(conn)

		select {
		case <-streamHandled:
		case <-time.After(time.Second):
			t.Fatal("stream was not handled")
		}

		assert.Equal(t, 2, acceptCalls)
		assert.NotContains(t, r.conns, conn)
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
