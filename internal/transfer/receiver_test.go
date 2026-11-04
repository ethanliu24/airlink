package transfer

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const receiverTestTimeout = 2 * time.Second

func TestNewReceiver(t *testing.T) {
	t.Parallel()

	transport := &quic.Transport{}

	receive := func(Stream) (TransferId, error) {
		return "transfer-123", nil
	}

	receiver := NewReceiver(transport, receive)

	require.NotNil(t, receiver)
	require.NotNil(t, receiver.listen)
	require.NotNil(t, receiver.receive)
	require.NotNil(t, receiver.conns)
	assert.Nil(t, receiver.listener)
	assert.False(t, receiver.isListening)
	assert.Empty(t, receiver.conns)
}

func TestHandleStream(t *testing.T) {
	t.Parallel()

	t.Run("delegates stream to manager", func(t *testing.T) {
		t.Parallel()

		stream := &mockStream{}

		received := make(chan Stream, 1)

		receiver := &Receiver{
			receive: func(got Stream) (TransferId, error) {
				received <- got
				return "transfer-123", nil
			},
		}

		receiver.handleStream(stream)

		select {
		case got := <-received:
			assert.Same(t, stream, got)
		case <-time.After(receiverTestTimeout):
			t.Fatal("receive callback was not called")
		}
	})

	t.Run("delegates stream even when manager returns error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("manager receive failed")
		stream := &mockStream{}

		called := false

		receiver := &Receiver{
			receive: func(got Stream) (TransferId, error) {
				assert.Same(t, stream, got)
				called = true
				return "", expectedErr
			},
		}

		// handleStream delegates the stream. The manager owns
		// handling errors and stream cleanup.
		receiver.handleStream(stream)

		assert.True(t, called)
	})
}

func TestHandleConnection(t *testing.T) {
	t.Parallel()

	t.Run("removes connection after stream accept error", func(t *testing.T) {
		t.Parallel()

		conn := &mockConn{
			acceptStreamFunc: func(context.Context) (Stream, error) {
				return nil, errors.New("connection closed")
			},
			closeWithErrFunc: func(
				quic.ApplicationErrorCode,
				string,
			) error {
				return nil
			},
		}

		receiver := &Receiver{
			conns: map[Conn]struct{}{
				conn: {},
			},
		}

		receiver.handleConnection(conn)

		receiver.mu.Lock()
		_, exists := receiver.conns[conn]
		receiver.mu.Unlock()

		assert.False(t, exists)
	})

	t.Run("closes connection normally", func(t *testing.T) {
		t.Parallel()

		closeCalled := make(chan struct{})

		conn := &mockConn{
			acceptStreamFunc: func(context.Context) (Stream, error) {
				return nil, errors.New("connection closed")
			},
			closeWithErrFunc: func(
				code quic.ApplicationErrorCode,
				msg string,
			) error {
				assert.Equal(t, quic.ApplicationErrorCode(0), code)
				assert.Equal(t, "receiver connection closed normally", msg)

				close(closeCalled)
				return nil
			},
		}

		receiver := &Receiver{
			conns: map[Conn]struct{}{
				conn: {},
			},
		}

		receiver.handleConnection(conn)

		select {
		case <-closeCalled:
		case <-time.After(receiverTestTimeout):
			t.Fatal("connection was not closed")
		}

		receiver.mu.Lock()
		_, exists := receiver.conns[conn]
		receiver.mu.Unlock()

		assert.False(t, exists)
	})

	t.Run("delegates accepted streams", func(t *testing.T) {
		t.Parallel()

		stream := &mockStream{}
		received := make(chan Stream, 1)

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

		receiver := &Receiver{
			conns: make(map[Conn]struct{}),
			receive: func(got Stream) (TransferId, error) {
				received <- got
				return "transfer-123", nil
			},
		}

		receiver.conns[conn] = struct{}{}

		receiver.handleConnection(conn)

		select {
		case got := <-received:
			assert.Same(t, stream, got)
		case <-time.After(receiverTestTimeout):
			t.Fatal("accepted stream was not delegated")
		}

		assert.Equal(t, 2, acceptCalls)

		receiver.mu.Lock()
		_, exists := receiver.conns[conn]
		receiver.mu.Unlock()

		assert.False(t, exists)
	})
}

func TestReceiverListen(t *testing.T) {
	t.Parallel()

	t.Run("listen error", func(t *testing.T) {
		t.Parallel()

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

	t.Run("success starts listener accept loop", func(t *testing.T) {
		t.Parallel()

		acceptCalled := make(chan struct{}, 1)

		listener := &mockListener{
			acceptFunc: func(context.Context) (Conn, error) {
				acceptCalled <- struct{}{}
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

		err := receiver.Listen(&tls.Config{}, &quic.Config{})

		require.NoError(t, err)

		receiver.mu.Lock()
		storedListener := receiver.listener
		isListening := receiver.isListening
		receiver.mu.Unlock()

		assert.Same(t, listener, storedListener)
		assert.True(t, isListening)

		select {
		case <-acceptCalled:
		case <-time.After(receiverTestTimeout):
			t.Fatal("listener accept loop did not start")
		}
	})

	t.Run("errors if listener is already listening", func(t *testing.T) {
		t.Parallel()

		acceptCalled := make(chan struct{}, 1)
		listenCalls := 0

		listener := &mockListener{
			acceptFunc: func(context.Context) (Conn, error) {
				acceptCalled <- struct{}{}
				return nil, errors.New("listener closed")
			},
			closeFunc: func() error {
				return nil
			},
		}

		receiver := &Receiver{
			listen: func(*tls.Config, *quic.Config) (Listener, error) {
				listenCalls++
				return listener, nil
			},
		}

		err := receiver.Listen(&tls.Config{}, &quic.Config{})
		require.NoError(t, err)

		err = receiver.Listen(&tls.Config{}, &quic.Config{})

		require.ErrorIs(t, err, ReceiverAlreadyListeningError)
		assert.Equal(t, 1, listenCalls)

		select {
		case <-acceptCalled:
		case <-time.After(receiverTestTimeout):
			t.Fatal("listener accept loop did not start")
		}
	})

	t.Run("can listen again after cleanup", func(t *testing.T) {
		t.Parallel()

		firstAcceptCalled := make(chan struct{}, 1)
		secondAcceptCalled := make(chan struct{}, 1)

		firstListener := &mockListener{
			acceptFunc: func(context.Context) (Conn, error) {
				firstAcceptCalled <- struct{}{}
				return nil, errors.New("first listener closed")
			},
			closeFunc: func() error {
				return nil
			},
		}

		secondListener := &mockListener{
			acceptFunc: func(context.Context) (Conn, error) {
				secondAcceptCalled <- struct{}{}
				return nil, errors.New("second listener closed")
			},
			closeFunc: func() error {
				return nil
			},
		}

		listenCalls := 0

		receiver := &Receiver{
			listen: func(*tls.Config, *quic.Config) (Listener, error) {
				listenCalls++

				if listenCalls == 1 {
					return firstListener, nil
				}

				return secondListener, nil
			},
		}

		require.NoError(t, receiver.Listen(&tls.Config{}, &quic.Config{}))

		select {
		case <-firstAcceptCalled:
		case <-time.After(receiverTestTimeout):
			t.Fatal("first listener accept loop did not start")
		}

		receiver.Cleanup()

		receiver.mu.Lock()
		isListening := receiver.isListening
		receiver.mu.Unlock()

		assert.False(t, isListening)

		require.NoError(t, receiver.Listen(&tls.Config{}, &quic.Config{}))

		receiver.mu.Lock()
		storedListener := receiver.listener
		receiver.mu.Unlock()

		assert.Same(t, secondListener, storedListener)

		select {
		case <-secondAcceptCalled:
		case <-time.After(receiverTestTimeout):
			t.Fatal("second listener accept loop did not start")
		}
	})
}

func TestReceiverReceive(t *testing.T) {
	t.Parallel()

	t.Run("registers accepted connection and handles it", func(t *testing.T) {
		t.Parallel()

		handlerStarted := make(chan struct{})
		releaseHandler := make(chan struct{})
		connectionClosed := make(chan struct{})

		var closeOnce sync.Once

		conn := &mockConn{
			acceptStreamFunc: func(context.Context) (Stream, error) {
				close(handlerStarted)
				<-releaseHandler
				return nil, errors.New("connection closed")
			},
			closeWithErrFunc: func(
				quic.ApplicationErrorCode,
				string,
			) error {
				closeOnce.Do(func() {
					close(connectionClosed)
				})
				return nil
			},
		}

		acceptCalls := 0

		listener := &mockListener{
			acceptFunc: func(context.Context) (Conn, error) {
				acceptCalls++

				if acceptCalls == 1 {
					return conn, nil
				}

				return nil, errors.New("listener closed")
			},
		}

		receiver := &Receiver{
			conns: make(map[Conn]struct{}),
		}

		receiver.recieve(listener)

		select {
		case <-handlerStarted:
		case <-time.After(receiverTestTimeout):
			t.Fatal("connection handler did not start")
		}

		receiver.mu.Lock()
		_, exists := receiver.conns[conn]
		receiver.mu.Unlock()

		require.True(t, exists, "accepted connection should be registered")

		close(releaseHandler)

		select {
		case <-connectionClosed:
		case <-time.After(receiverTestTimeout):
			t.Fatal("connection handler did not close connection")
		}

		require.Eventually(t, func() bool {
			receiver.mu.Lock()
			defer receiver.mu.Unlock()

			_, exists := receiver.conns[conn]
			return !exists
		}, receiverTestTimeout, time.Millisecond)
	})
}

func TestReceiverCleanup(t *testing.T) {
	t.Parallel()

	t.Run("nil resources do not panic", func(t *testing.T) {
		t.Parallel()

		receiver := &Receiver{}

		assert.NotPanics(t, func() {
			receiver.Cleanup()
		})
	})

	t.Run("closes listener and clears listening state", func(t *testing.T) {
		t.Parallel()

		closed := false

		listener := &mockListener{
			closeFunc: func() error {
				closed = true
				return nil
			},
			acceptFunc: func(context.Context) (Conn, error) {
				return nil, errors.New("unexpected accept")
			},
		}

		receiver := &Receiver{
			listener:    listener,
			isListening: true,
		}

		receiver.Cleanup()

		assert.True(t, closed)
		assert.Nil(t, receiver.listener)
		assert.False(t, receiver.isListening)
	})

	t.Run("closes active connections", func(t *testing.T) {
		t.Parallel()

		var (
			mu                sync.Mutex
			closedConnections []Conn
		)

		conn1 := &mockConn{}
		conn2 := &mockConn{}

		conn1.closeWithErrFunc = func(
			code quic.ApplicationErrorCode,
			msg string,
		) error {
			assert.Equal(t, quic.ApplicationErrorCode(1), code)
			assert.Equal(t, "receiver tearing down", msg)

			mu.Lock()
			closedConnections = append(closedConnections, conn1)
			mu.Unlock()

			return nil
		}

		conn2.closeWithErrFunc = func(
			code quic.ApplicationErrorCode,
			msg string,
		) error {
			assert.Equal(t, quic.ApplicationErrorCode(1), code)
			assert.Equal(t, "receiver tearing down", msg)

			mu.Lock()
			closedConnections = append(closedConnections, conn2)
			mu.Unlock()

			return nil
		}

		receiver := &Receiver{
			conns: map[Conn]struct{}{
				conn1: {},
				conn2: {},
			},
		}

		receiver.Cleanup()

		mu.Lock()
		assert.ElementsMatch(t, []Conn{conn1, conn2}, closedConnections)
		mu.Unlock()

		assert.Empty(t, receiver.conns)
	})
}
