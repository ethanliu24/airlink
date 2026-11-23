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
	"net"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const senderTestIP = "127.0.0.1"

var tlsConfig *tls.Config = security.GenerateTestTLSConfig()
var quicConfig *quic.Config = getTestQuicConfig()

func senderTestAddr() net.Addr {
	return &net.UDPAddr{
		IP:   net.ParseIP(senderTestIP),
		Port: 54321,
	}
}

func getTestQuicConfig() *quic.Config {
	return &quic.Config{
		MaxIdleTimeout:  30 * time.Second,
		KeepAlivePeriod: 15 * time.Second,
	}
}

func mockDialErr(_ context.Context, _ net.Addr, _ *tls.Config, _ *quic.Config) (Conn, error) {
	return nil, errors.New("mock dial connection failed")
}

func newSenderAuthStream(
	t *testing.T,
	remotePeerID string,
	version uint32,
) (*mockStream, *bytes.Buffer, *mockStreamState) {
	t.Helper()

	// Prepare the simulated remote response.
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

func newSenderTLSState(t *testing.T) (*security.Identity, tls.ConnectionState) {
	t.Helper()

	identity, err := security.GenerateIdentity()
	require.NoError(t, err)

	cert := identity.TLSCertificate().Leaf

	return identity, tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{cert},
	}
}

func TestNewSender(t *testing.T) {
	t.Parallel()

	transport := &quic.Transport{}

	send := func(transfer.TransferStream, string) (transfer.TransferId, error) {
		return "transfer-123", nil
	}

	transportSender := NewSender(
		"test",
		transport,
		send,
	)

	sender, ok := transportSender.(*sender)

	require.True(t, ok)
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

		addr := senderTestAddr().(*net.UDPAddr)

		sendCalled := false

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

	t.Run("authentication stream error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("open authentication stream failed")

		closeCalled := make(chan struct{}, 1)
		sendCalled := make(chan struct{}, 1)

		conn := &mockConn{
			openStreamSyncFunc: func(
				context.Context,
			) (transfer.TransferStream, error) {
				return nil, expectedErr
			},
			closeWithErrFunc: func(
				quic.ApplicationErrorCode,
				string,
			) error {
				closeCalled <- struct{}{}
				return nil
			},
		}

		sender := &sender{
			id: metadata.PeerId("sender-peer"),
			dial: func(
				context.Context,
				net.Addr,
				*tls.Config,
				*quic.Config,
			) (Conn, error) {
				return conn, nil
			},
			conns: make(map[string]Conn),
			send: func(transfer.TransferStream, string) (transfer.TransferId, error) {
				sendCalled <- struct{}{}
				return "", nil
			},
		}

		err := sender.Send(
			senderTestAddr().(*net.UDPAddr),
			"test.txt",
			tlsConfig,
			quicConfig,
		)

		require.ErrorIs(t, err, expectedErr)
		assert.Empty(t, sender.conns)

		select {
		case <-closeCalled:
			// Connection was closed after authentication setup failed.
		default:
			t.Fatal("expected connection to be closed")
		}

		select {
		case <-sendCalled:
			t.Fatal("transfer should not start after authentication failure")
		default:
		}
	})

	t.Run("authentication deadline error", func(t *testing.T) {
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

		closeCalled := make(chan struct{}, 1)

		conn := &mockConn{
			openStreamSyncFunc: func(
				context.Context,
			) (transfer.TransferStream, error) {
				return authStream, nil
			},

			closeWithErrFunc: func(
				quic.ApplicationErrorCode,
				string,
			) error {
				closeCalled <- struct{}{}
				return nil
			},
		}

		sender := &sender{
			id: metadata.PeerId("sender-peer"),
			dial: func(
				context.Context,
				net.Addr,
				*tls.Config,
				*quic.Config,
			) (Conn, error) {
				return conn, nil
			},
			conns: make(map[string]Conn),
			send: func(transfer.TransferStream, string) (transfer.TransferId, error) {
				return "", nil
			},
		}

		err := sender.Send(
			senderTestAddr().(*net.UDPAddr),
			"test.txt",
			tlsConfig,
			quicConfig,
		)

		require.ErrorIs(t, err, expectedErr)
		assert.Empty(t, sender.conns)
		assert.True(t, closeCalled != nil)

		select {
		case <-closeCalled:
		default:
			t.Fatal("expected connection to be closed")
		}
	})

	t.Run("peer identity exchange error", func(t *testing.T) {
		t.Parallel()

		remoteIdentity, connectionState := newSenderTLSState(t)
		authStream, _, streamState := newSenderAuthStream(
			t,
			"remote-peer",
			protocolVersion+1,
		)

		closeCalled := make(chan struct{}, 1)

		conn := &mockConn{
			openStreamSyncFunc: func(
				context.Context,
			) (transfer.TransferStream, error) {
				return authStream, nil
			},

			connectionStateFunc: func() tls.ConnectionState {
				return connectionState
			},

			closeWithErrFunc: func(
				quic.ApplicationErrorCode,
				string,
			) error {
				closeCalled <- struct{}{}
				return nil
			},
		}

		sendCalled := false

		sender := &sender{
			id: metadata.PeerId(testLocalPeerID),
			dial: func(
				context.Context,
				net.Addr,
				*tls.Config,
				*quic.Config,
			) (Conn, error) {
				return conn, nil
			},
			conns: make(map[string]Conn),
			send: func(transfer.TransferStream, string) (transfer.TransferId, error) {
				sendCalled = true
				return "", nil
			},
		}

		require.NotNil(t, remoteIdentity)

		err := sender.Send(
			senderTestAddr().(*net.UDPAddr),
			"test.txt",
			tlsConfig,
			quicConfig,
		)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported peer protocol version")
		assert.Empty(t, sender.conns)
		assert.False(t, sendCalled)
		assert.True(t, streamState.closed)

		select {
		case <-closeCalled:
		default:
			t.Fatal("expected connection to be closed after exchange failure")
		}
	})

	t.Run("successful identity exchange and transfer delegation", func(t *testing.T) {
		t.Parallel()

		const (
			filename   = "test.txt"
			transferID = "transfer-123"
			localID    = metadata.PeerId(testLocalPeerID)
			remoteID   = metadata.PeerId(testRemotePeerID)
		)

		remoteIdentity, connectionState := newSenderTLSState(t)
		authStream, outgoing, authState := newSenderAuthStream(
			t,
			remoteID,
			protocolVersion,
		)

		transferStream := &mockStream{
			closeFunc: func() error {
				return nil
			},
		}

		expectedConn := &mockConn{
			openStreamSyncFunc: func(
				context.Context,
			) (transfer.TransferStream, error) {
				return authStream, nil
			},
			connectionStateFunc: func() tls.ConnectionState {
				return connectionState
			},
			openStreamFunc: func() (transfer.TransferStream, error) {
				return transferStream, nil
			},
			closeWithErrFunc: func(
				code quic.ApplicationErrorCode,
				message string,
			) error {
				t.Errorf("CloseWithError unexpectedly called: %s", message)
				return nil
			},
		}

		type sendCall struct {
			stream   transfer.TransferStream
			filename string
		}

		sendCalled := make(chan sendCall, 1)

		sender := &sender{
			id: localID,
			dial: func(
				context.Context,
				net.Addr,
				*tls.Config,
				*quic.Config,
			) (Conn, error) {
				return expectedConn, nil
			},
			conns: make(map[string]Conn),
			send: func(
				gotStream transfer.TransferStream,
				gotFilename string,
			) (transfer.TransferId, error) {
				sendCalled <- sendCall{
					stream:   gotStream,
					filename: gotFilename,
				}

				return transfer.TransferId(transferID), nil
			},
		}

		err := sender.Send(
			senderTestAddr().(*net.UDPAddr),
			filename,
			tlsConfig,
			quicConfig,
		)

		require.NoError(t, err)

		// Verify authentication stream lifecycle.
		assert.True(t, authState.closed)
		assert.True(t, authState.deadlineSet)
		assert.False(t, authState.deadline.IsZero())

		// Verify outgoing PeerHello.
		var hello models.PeerHello
		require.NoError(t, wire.ReadMessage(outgoing, &hello))

		assert.Equal(t, string(localID), hello.GetPeerId())
		assert.Equal(t, protocolVersion, hello.GetProtocolVersion())

		// Verify remote identity was available in the TLS state.
		assert.NotNil(t, remoteIdentity)

		// Wait for asynchronous transfer delegation.
		select {
		case call := <-sendCalled:
			assert.Same(t, transferStream, call.stream)
			assert.Equal(t, filename, call.filename)

		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for transfer delegation")
		}

		// Verify connection registration.
		sender.mu.Lock()
		conn, exists := sender.conns[senderTestAddr().String()]
		sender.mu.Unlock()

		require.True(t, exists)
		assert.Same(t, expectedConn, conn)
	})

	t.Run("transfer stream open error", func(t *testing.T) {
		t.Parallel()

		_, connectionState := newSenderTLSState(t)

		authStream, _, _ := newSenderAuthStream(
			t,
			testLocalPeerID,
			protocolVersion,
		)

		openAttempted := make(chan struct{}, 1)

		conn := &mockConn{
			openStreamSyncFunc: func(ctx context.Context) (transfer.TransferStream, error) {
				return authStream, nil
			},
			openStreamFunc: func() (transfer.TransferStream, error) {
				openAttempted <- struct{}{}
				return nil, errors.New("transfer stream open failed")
			},
			connectionStateFunc: func() tls.ConnectionState {
				return connectionState
			},
		}

		sendCalled := make(chan struct{}, 1)

		sender := &sender{
			id: metadata.PeerId(testRemotePeerID),
			dial: func(
				context.Context,
				net.Addr,
				*tls.Config,
				*quic.Config,
			) (Conn, error) {
				return conn, nil
			},
			conns: make(map[string]Conn),
			send: func(transfer.TransferStream, string) (transfer.TransferId, error) {
				sendCalled <- struct{}{}
				return "", nil
			},
		}

		addr := senderTestAddr().(*net.UDPAddr)

		err := sender.Send(addr, "test.txt", tlsConfig, quicConfig)
		require.NoError(t, err)

		// Transfer stream creation happens asynchronously.
		select {
		case <-openAttempted:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for transfer stream creation")
		}

		// Connection should remain registered because identity exchange succeeded.
		sender.mu.Lock()
		storedConn, exists := sender.conns[addr.String()]
		sender.mu.Unlock()

		require.True(t, exists)
		assert.Same(t, conn, storedConn)

		// Transfer manager should not be called when stream creation fails.
		select {
		case <-sendCalled:
			t.Fatal("send manager called despite stream creation failure")
		default:
		}
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

	sender.CleanUp()

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
		sender.CleanUp()
	})

	assert.Empty(t, sender.conns)
}
