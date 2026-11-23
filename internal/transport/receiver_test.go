package transport

import (
	"airlink/internal/metadata"
	"airlink/internal/security"
	"airlink/internal/security/models"
	"airlink/internal/transfer"
	"airlink/internal/wire"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const receiverTestTimeout = 2 * time.Second

func newReceiverAuthStream(
	t *testing.T,
	remotePeerID string,
	version uint32,
) (*mockStream, *bytes.Buffer, *mockStreamState) {
	t.Helper()

	incoming := &bytes.Buffer{}

	require.NoError(t, wire.WriteMessage(incoming, &models.PeerHello{
		PeerId:          remotePeerID,
		ProtocolVersion: version,
	}))

	reader := bytes.NewReader(incoming.Bytes())
	outgoing := &bytes.Buffer{}

	state := &mockStreamState{}

	stream := &mockStream{
		readFunc:  reader.Read,
		writeFunc: outgoing.Write,

		closeFunc: func() error {
			state.closed = true
			return nil
		},

		abortFunc: func() {},

		setDeadlineFunc: func(deadline time.Time) error {
			state.deadlineSet = true
			state.deadline = deadline
			return nil
		},
	}

	return stream, outgoing, state
}

func newReceiverTLSState(t *testing.T) (*security.Identity, tls.ConnectionState) {
	t.Helper()

	identity, err := security.GenerateIdentity()
	require.NoError(t, err)

	cert := identity.TLSCertificate().Leaf
	require.NotNil(t, cert)

	return identity, tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{cert},
	}
}

func TestNewReceiver(t *testing.T) {
	t.Parallel()

	transport := &quic.Transport{}

	receive := func(transfer.TransferStream) (transfer.TransferId, error) {
		return "transfer-123", nil
	}

	transportReceiver := NewReceiver("test", transport, receive)
	receiver, ok := transportReceiver.(*receiver)

	require.True(t, ok)
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

		received := make(chan transfer.TransferStream, 1)

		receiver := &receiver{
			receive: func(got transfer.TransferStream) (transfer.TransferId, error) {
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

		receiver := &receiver{
			receive: func(got transfer.TransferStream) (transfer.TransferId, error) {
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
			acceptStreamFunc: func(context.Context) (transfer.TransferStream, error) {
				return nil, errors.New("connection closed")
			},
			closeWithErrFunc: func(
				quic.ApplicationErrorCode,
				string,
			) error {
				return nil
			},
		}

		receiver := &receiver{
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
			acceptStreamFunc: func(context.Context) (transfer.TransferStream, error) {
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

		receiver := &receiver{
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
		received := make(chan transfer.TransferStream, 1)

		acceptCalls := 0

		conn := &mockConn{
			acceptStreamFunc: func(context.Context) (transfer.TransferStream, error) {
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

		receiver := &receiver{
			conns: make(map[Conn]struct{}),
			receive: func(got transfer.TransferStream) (transfer.TransferId, error) {
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

func TestReceiverAuthenticatePeer(t *testing.T) {
	t.Parallel()

	t.Run("successful authentication", func(t *testing.T) {
		t.Parallel()

		remoteID := metadata.PeerId(testRemotePeerID)
		localID := metadata.PeerId(testLocalPeerID)

		identity, connectionState := newReceiverTLSState(t)

		authStream, outgoing, streamState := newReceiverAuthStream(
			t,
			string(remoteID),
			protocolVersion,
		)

		acceptCalls := 0

		conn := &mockConn{
			acceptStreamFunc: func(context.Context) (transfer.TransferStream, error) {
				acceptCalls++
				return authStream, nil
			},
			connectionStateFunc: func() tls.ConnectionState {
				return connectionState
			},
		}

		receiver := &receiver{
			id: localID,
		}

		remote, err := receiver.authenticatePeer(conn)

		require.NoError(t, err)
		require.NotNil(t, remote)

		assert.Equal(t, remoteID, remote.PeerID())
		assert.Equal(t, identity.Fingerprint(), remote.Fingerprint())
		assert.Equal(t, 1, acceptCalls)

		// Verify authentication stream lifecycle.
		assert.True(t, streamState.closed)
		assert.True(t, streamState.deadlineSet)
		assert.False(t, streamState.deadline.IsZero())

		// Verify the receiver sent its own PeerHello.
		var sentHello models.PeerHello

		require.NoError(t, wire.ReadMessage(outgoing, &sentHello))

		assert.Equal(t, string(localID), sentHello.GetPeerId())
		assert.Equal(t, protocolVersion, sentHello.GetProtocolVersion())
	})

	t.Run("accept stream error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("accept authentication stream failed")

		conn := &mockConn{
			acceptStreamFunc: func(context.Context) (transfer.TransferStream, error) {
				return nil, expectedErr
			},
		}

		receiver := &receiver{
			id: metadata.PeerId("local-peer"),
		}

		remote, err := receiver.authenticatePeer(conn)

		require.ErrorIs(t, err, expectedErr)
		assert.Nil(t, remote)
	})

	t.Run("set deadline error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("set deadline failed")

		authStream := &mockStream{
			closeFunc: func() error {
				return nil
			},
			setDeadlineFunc: func(time.Time) error {
				return expectedErr
			},
		}

		conn := &mockConn{
			acceptStreamFunc: func(context.Context) (transfer.TransferStream, error) {
				return authStream, nil
			},
		}

		receiver := &receiver{
			id: metadata.PeerId("local-peer"),
		}

		remote, err := receiver.authenticatePeer(conn)

		require.ErrorIs(t, err, expectedErr)
		assert.Nil(t, remote)
	})

	t.Run("invalid remote hello", func(t *testing.T) {
		t.Parallel()

		_, connectionState := newReceiverTLSState(t)

		authStream, _, streamState := newReceiverAuthStream(
			t,
			testRemotePeerID,
			protocolVersion+1,
		)

		conn := &mockConn{
			acceptStreamFunc: func(context.Context) (transfer.TransferStream, error) {
				return authStream, nil
			},
			connectionStateFunc: func() tls.ConnectionState {
				return connectionState
			},
		}

		receiver := &receiver{
			id: metadata.PeerId(testLocalPeerID),
		}

		remote, err := receiver.authenticatePeer(conn)

		require.Error(t, err)
		assert.ErrorContains(t, err, "unsupported peer protocol version")
		assert.Nil(t, remote)
		assert.True(t, streamState.closed)
	})

	t.Run("missing remote certificate", func(t *testing.T) {
		t.Parallel()

		authStream, _, streamState := newReceiverAuthStream(
			t,
			testRemotePeerID,
			protocolVersion,
		)

		conn := &mockConn{
			acceptStreamFunc: func(context.Context) (transfer.TransferStream, error) {
				return authStream, nil
			},
			connectionStateFunc: func() tls.ConnectionState {
				return tls.ConnectionState{}
			},
		}

		receiver := &receiver{
			id: metadata.PeerId(testLocalPeerID),
		}

		remote, err := receiver.authenticatePeer(conn)

		require.Error(t, err)
		assert.ErrorContains(t, err, "no peer certificates presented")
		assert.Nil(t, remote)
		assert.True(t, streamState.closed)
	})
}

func TestReceiverListen(t *testing.T) {
	t.Parallel()

	t.Run("listen error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("listen failed")

		receiver := &receiver{
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

		receiver := &receiver{
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

		receiver := &receiver{
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

		receiver := &receiver{
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

		receiver.CleanUp()

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

	t.Run("authenticates connection before handling it", func(t *testing.T) {
		t.Parallel()

		localID := metadata.PeerId(testLocalPeerID)

		_, connectionState := newReceiverTLSState(t)

		authStream, outgoing, authState := newReceiverAuthStream(
			t,
			testRemotePeerID,
			protocolVersion,
		)

		handlerStarted := make(chan struct{})
		releaseHandler := make(chan struct{})
		connectionClosed := make(chan struct{})

		var (
			acceptMu    sync.Mutex
			acceptCalls int
		)

		var closeOnce sync.Once

		conn := &mockConn{
			acceptStreamFunc: func(context.Context) (transfer.TransferStream, error) {
				acceptMu.Lock()
				acceptCalls++
				call := acceptCalls
				acceptMu.Unlock()

				switch call {
				case 1:
					// First accepted stream must be the authentication stream.
					return authStream, nil

				case 2:
					// This call comes from handleConnection, meaning
					// authentication completed and handling started.
					close(handlerStarted)

					<-releaseHandler

					return nil, errors.New("connection closed")

				default:
					return nil, errors.New("unexpected stream accept")
				}
			},
			connectionStateFunc: func() tls.ConnectionState {
				return connectionState
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

		listenerAcceptCalls := 0

		listener := &mockListener{
			acceptFunc: func(context.Context) (Conn, error) {
				listenerAcceptCalls++

				if listenerAcceptCalls == 1 {
					return conn, nil
				}

				return nil, errors.New("listener closed")
			},
		}

		receiver := &receiver{
			id:    localID,
			conns: make(map[Conn]struct{}),
		}

		// The accept loop returns when the mock listener closes.
		// handleConnection remains blocked until releaseHandler.
		receiver.recieve(listener)

		select {
		case <-handlerStarted:
		case <-time.After(receiverTestTimeout):
			t.Fatal("connection handler did not start after authentication")
		}

		// Verify that the connection was registered after authentication.
		receiver.mu.Lock()
		_, exists := receiver.conns[conn]
		receiver.mu.Unlock()

		require.True(t, exists)

		// Verify authentication stream was closed.
		assert.True(t, authState.closed)
		assert.True(t, authState.deadlineSet)

		// Verify the local identity was sent.
		var sentHello models.PeerHello

		require.NoError(t, wire.ReadMessage(outgoing, &sentHello))

		assert.Equal(t, string(localID), sentHello.GetPeerId())
		assert.Equal(t, protocolVersion, sentHello.GetProtocolVersion())

		// Avoid leaking the handler goroutine.
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

	t.Run("rejects connection when authentication fails", func(t *testing.T) {
		t.Parallel()

		_, connectionState := newReceiverTLSState(t)

		authStream, _, authState := newReceiverAuthStream(
			t,
			"remote-peer",
			protocolVersion+1,
		)

		connectionClosed := make(chan struct{}, 1)
		acceptCalls := 0

		conn := &mockConn{
			acceptStreamFunc: func(context.Context) (transfer.TransferStream, error) {
				acceptCalls++
				return authStream, nil
			},

			connectionStateFunc: func() tls.ConnectionState {
				return connectionState
			},

			closeWithErrFunc: func(
				quic.ApplicationErrorCode,
				string,
			) error {
				connectionClosed <- struct{}{}
				return nil
			},
		}

		listenerAcceptCalls := 0

		listener := &mockListener{
			acceptFunc: func(context.Context) (Conn, error) {
				listenerAcceptCalls++

				if listenerAcceptCalls == 1 {
					return conn, nil
				}

				return nil, errors.New("listener closed")
			},
		}

		receiver := &receiver{
			id:    metadata.PeerId("local-peer"),
			conns: make(map[Conn]struct{}),
		}

		receiver.recieve(listener)

		select {
		case <-connectionClosed:
		case <-time.After(receiverTestTimeout):
			t.Fatal("connection was not closed after authentication failure")
		}

		// Failed authentication must not register the connection.
		receiver.mu.Lock()
		_, exists := receiver.conns[conn]
		receiver.mu.Unlock()

		assert.False(t, exists)

		// handleConnection must never start.
		assert.Equal(t, 1, acceptCalls)

		// Authentication stream must still be cleaned up.
		assert.True(t, authState.closed)
		assert.True(t, authState.deadlineSet)
	})
}

func TestReceiverCleanup(t *testing.T) {
	t.Parallel()

	t.Run("nil resources do not panic", func(t *testing.T) {
		t.Parallel()

		receiver := &receiver{}

		assert.NotPanics(t, func() {
			receiver.CleanUp()
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

		receiver := &receiver{
			listener:    listener,
			isListening: true,
		}

		receiver.CleanUp()

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

		receiver := &receiver{
			conns: map[Conn]struct{}{
				conn1: {},
				conn2: {},
			},
		}

		receiver.CleanUp()

		mu.Lock()
		assert.ElementsMatch(t, []Conn{conn1, conn2}, closedConnections)
		mu.Unlock()

		assert.Empty(t, receiver.conns)
	})
}
