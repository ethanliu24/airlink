package security

import (
	"crypto/sha256"
	"crypto/x509"
	"fmt"
	"strings"
)

const maxPeerIDLength = 128

type PeerIdentity struct {
	peerID      string
	fingerprint [32]byte
}

func (p PeerIdentity) PeerID() string {
	return p.peerID
}

func (p PeerIdentity) Fingerprint() [32]byte {
	return p.fingerprint
}

func NewPeerIdentity(peerId string, certificate *x509.Certificate) (*PeerIdentity, error) {
	if strings.TrimSpace(peerId) == "" {
		return nil, fmt.Errorf("peer identity: peer ID is empty")
	}

	if len(peerId) > maxPeerIDLength {
		return nil, fmt.Errorf("peer identity: peer ID exceeds maximum length")
	}

	if certificate == nil {
		return nil, fmt.Errorf("peer identity: certificate is nil")
	}

	// validate the certificate before extracting its identity
	if err := ValidateCertificate(certificate); err != nil {
		return nil, fmt.Errorf("peer identity: invalid certificate: %w", err)
	}

	fingerprint := sha256.Sum256(certificate.RawSubjectPublicKeyInfo)

	return &PeerIdentity{
		peerID:      peerId,
		fingerprint: fingerprint,
	}, nil
}
