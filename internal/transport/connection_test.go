package transport

import (
	"airlink/internal/metadata"
	"airlink/internal/security"
	"airlink/internal/security/models"
	"airlink/internal/wire"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testLocalPeerID  = "0190a1b2-c3d4-7e5f-8a9b-0123456789ab"
	testRemotePeerID = "0190a1b2-c3d4-7e5f-8a9b-0123456789ac"
)

// exchangeWithResponse simulates a remote peer responding to PeerHello.
// It returns the resulting remote identity, the request received by the
// simulated server, and any error returned by exchangePeerIdentity.
func exchangeWithResponse(
	t *testing.T,
	localPeerID metadata.PeerId,
	connectionState tls.ConnectionState,
	response *models.PeerHello,
) (*security.PeerIdentity, *models.PeerHello, error) {
	t.Helper()

	client, server := net.Pipe()

	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	type serverResult struct {
		request *models.PeerHello
		err     error
	}

	serverDone := make(chan serverResult, 1)

	// Simulate the remote peer.
	go func() {
		var request models.PeerHello

		if err := wire.ReadMessage(server, &request); err != nil {
			serverDone <- serverResult{
				err: err,
			}
			return
		}

		if err := wire.WriteMessage(server, response); err != nil {
			serverDone <- serverResult{
				err: err,
			}
			return
		}

		serverDone <- serverResult{
			request: &request,
		}
	}()

	// Execute the function under test.
	remoteIdentity, exchangeErr := exchangePeerIdentity(
		client,
		localPeerID,
		connectionState,
	)

	// Wait for the simulated server to finish.
	select {
	case result := <-serverDone:
		require.NoError(t, result.err, "simulated server failed")
		require.NotNil(t, result.request)

		return remoteIdentity, result.request, exchangeErr

	case <-time.After(2 * time.Second):
		t.Fatal("simulated server timed out")
		return nil, nil, nil
	}
}

func startSimulatedServer(
    conn net.Conn,
    response *models.PeerHello,
) <-chan error {
    done := make(chan error, 1)

    go func() {
        defer conn.Close()

        var hello models.PeerHello
        if err := wire.ReadMessage(conn, &hello); err != nil {
            done <- err
            return
        }

        done <- wire.WriteMessage(conn, response)
    }()

    return done
}

func TestExchangePeerIdentity(t *testing.T) {
	localPeerID := metadata.PeerId(testLocalPeerID)
	remotePeerID := metadata.PeerId(testRemotePeerID)

	identity, err := security.GenerateIdentity()
	require.NoError(t, err)

	cert := identity.TLSCertificate().Leaf
	require.NotNil(t, cert)

	connectionState := tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{cert},
	}

	response := &models.PeerHello{
		PeerId:          string(remotePeerID),
		ProtocolVersion: protocolVersion,
	}

	remote, sent, err := exchangeWithResponse(
		t,
		localPeerID,
		connectionState,
		response,
	)

	require.NoError(t, err)
	require.NotNil(t, remote)

	// Verify outgoing PeerHello.
	assert.Equal(t, string(localPeerID), sent.GetPeerId())
	assert.Equal(t, protocolVersion, sent.GetProtocolVersion())

	// Verify remote PeerIdentity.
	assert.Equal(t, remotePeerID, remote.PeerID())
	assert.Equal(t, identity.Fingerprint(), remote.Fingerprint())
}

func TestExchangePeerIdentity_InvalidResponse(t *testing.T) {
	identity, err := security.GenerateIdentity()
	require.NoError(t, err)

	cert := identity.TLSCertificate().Leaf
	require.NotNil(t, cert)

	connectionState := tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{cert},
	}

	tests := []struct {
		name    string
		peerID  string
		version uint32
	}{
		{
			name:    "empty peer ID",
			peerID:  "",
			version: protocolVersion,
		},
		{
			name:    "whitespace peer ID",
			peerID:  " remote-peer ",
			version: protocolVersion,
		},
		{
			name:    "peer ID too long",
			peerID:  strings.Repeat("a", metadata.MaxPeerIDLength+1),
			version: protocolVersion,
		},
		{
			name:    "unsupported protocol version",
			peerID:  "remote-peer",
			version: protocolVersion + 1,
		},
		{
			name:    "self connection",
			peerID:  "local-peer",
			version: protocolVersion,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := &models.PeerHello{
				PeerId:          tt.peerID,
				ProtocolVersion: tt.version,
			}

			client, server := net.Pipe()
			defer client.Close()

			serverDone := startSimulatedServer(server, response)

			_, err = exchangePeerIdentity(
				client,
				testLocalPeerID,
				connectionState,
			)

			require.Error(t, err)
			require.NoError(t, <-serverDone)
		})
	}
}

func TestExchangePeerIdentity_InvalidLocalInput(t *testing.T) {
	identity, err := security.GenerateIdentity()
	require.NoError(t, err)

	cert := identity.TLSCertificate().Leaf
	require.NotNil(t, cert)

	tests := []struct {
		name            string
		peerID          metadata.PeerId
		connectionState tls.ConnectionState
		wantErr         string
	}{
		{
			name:   "empty local peer ID",
			peerID: "",
			connectionState: tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{cert},
			},
			wantErr: "local peer ID is empty",
		},
		{
			name:    "missing peer certificate",
			peerID:  testLocalPeerID,
			wantErr: "no peer certificates presented",
		},
		{
			name:    "invalid remote peer ID",
			peerID:  "not-a-uuid",
			wantErr: "invalid local peer ID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer strings.Builder

			_, err := exchangePeerIdentity(
				&readWriterBuffer{builder: &buffer},
				tt.peerID,
				tt.connectionState,
			)

			require.Error(t, err)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestExchangePeerIdentity_InvalidCertificate(t *testing.T) {
	identity, err := security.GenerateIdentity()
	require.NoError(t, err)

	// Copy the certificate so we don't mutate the original.
	cert := *identity.TLSCertificate().Leaf
	cert.NotAfter = time.Now().Add(-time.Hour)

	connectionState := tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{&cert},
	}

	response := &models.PeerHello{
		PeerId:          testRemotePeerID,
		ProtocolVersion: protocolVersion,
	}

	remote, _, err := exchangeWithResponse(
		t,
		metadata.PeerId(testLocalPeerID),
		connectionState,
		response,
	)

	require.Error(t, err)
	assert.ErrorContains(t, err, "expired")
	assert.Nil(t, remote)
}

func TestExchangePeerIdentity_WriteError(t *testing.T) {
	identity, err := security.GenerateIdentity()
	require.NoError(t, err)

	writeErr := errors.New("simulated write failure")

	rw := &failingReadWriter{
		writeErr: writeErr,
	}

	connectionState := tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{
			identity.TLSCertificate().Leaf,
		},
	}

	_, err = exchangePeerIdentity(
		rw,
		metadata.PeerId(testLocalPeerID),
		connectionState,
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, writeErr)
}

type failingReadWriter struct {
	writeErr error
}

func (rw *failingReadWriter) Write([]byte) (int, error) {
	return 0, rw.writeErr
}

func (rw *failingReadWriter) Read([]byte) (int, error) {
	return 0, errors.New("unexpected read")
}

type readWriterBuffer struct {
	builder *strings.Builder
}

func (rw *readWriterBuffer) Write(p []byte) (int, error) {
	return rw.builder.Write(p)
}

func (rw *readWriterBuffer) Read([]byte) (int, error) {
	return 0, errors.New("unexpected read")
}
