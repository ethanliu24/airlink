package transport

import (
	"airlink/internal/metadata"
	"airlink/internal/security"
	"airlink/internal/security/models"
	"airlink/internal/transfer"
	"airlink/internal/wire"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/quic-go/quic-go"
)

// TODO centralize and figure it out properly
const (
	maxHandshakeTimeoutSeconds = 3 * time.Second
	maxPeerIDLength = 36

	protocolVersion = uint32(1)
)

type Conn interface {
	AcceptStream(context.Context) (transfer.TransferStream, error)
	OpenStream() (transfer.TransferStream, error)
	OpenStreamSync(context.Context) (transfer.TransferStream, error)
	ConnectionState() tls.ConnectionState
	CloseWithError(quic.ApplicationErrorCode, string) error
}

func exchangePeerIdentity(
	rw io.ReadWriter,
	peerId metadata.PeerId,
	connectionState tls.ConnectionState,
) (*security.PeerIdentity, error) {
	if peerId == "" {
		return nil, fmt.Errorf("peer identity exchange: local peer ID is empty")
	}

	_, err := uuid.Parse(peerId)
	if err != nil {
		return nil, fmt.Errorf("peer identity exchange: invalid local peer ID: %w", err)
	}

	if len(connectionState.PeerCertificates) == 0 {
		return nil, fmt.Errorf("peer identity exchange: no peer certificates presented")
	}

	// exchange
	message := &models.PeerHello{
		PeerId:          string(peerId),
		ProtocolVersion: protocolVersion,
	}

	err = wire.WriteMessage(rw, message)
	if err != nil {
		return nil, fmt.Errorf("peer identity exchange: write hello - %w", err)
	}

	var response models.PeerHello
	err = wire.ReadMessage(rw, &response)
	if err != nil {
		return nil, fmt.Errorf("peer identity exchange: read hello - %w", err)
	}

	// validate peer
	version := response.GetProtocolVersion()
	if version != protocolVersion {
		return nil, fmt.Errorf("peer identity exchange: unsupported peer protocol version: %d", version)
	}

	remotePeerId := metadata.PeerId(response.GetPeerId())
	if remotePeerId == "" || strings.TrimSpace(string(remotePeerId)) != string(remotePeerId) {
		return nil, fmt.Errorf("peer identity exchange: invalid remote peer ID")
	}

	if len(remotePeerId) > maxPeerIDLength {
		return nil, fmt.Errorf("peer identity exchange: remote peer ID too long")
	}

	_, err = uuid.Parse(response.PeerId)
	if err != nil {
		return nil, fmt.Errorf("peer identity exchange: invalid remote peer ID: %w", err)
	}

	if remotePeerId == peerId {
		return nil, fmt.Errorf("peer identity exchange: connected to self")
	}

	leafCert := connectionState.PeerCertificates[0]
	remoteIdentity, err := security.NewPeerIdentity(remotePeerId, leafCert)
	if err != nil {
		return nil, fmt.Errorf("construct remote peer identity: %w", err)
	}

	return remoteIdentity, nil
}
