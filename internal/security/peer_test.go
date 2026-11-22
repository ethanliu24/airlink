package security

import (
	"crypto/sha256"
	"crypto/x509"
	"strings"
	"testing"
)

func TestNewPeerIdentity(t *testing.T) {
    identity, err := GenerateIdentity()
    if err != nil {
        t.Fatalf("GenerateIdentity() error = %v", err)
    }

    cert := identity.TLSCertificate().Leaf

    peer, err := NewPeerIdentity("peer-123", cert)
    if err != nil {
        t.Fatalf("NewPeerIdentity() error = %v", err)
    }

    if peer.PeerID() != "peer-123" {
        t.Errorf("expected peer ID %q, got %q",
            "peer-123", peer.PeerID())
    }

    expected := sha256.Sum256(cert.RawSubjectPublicKeyInfo)

    if peer.Fingerprint() != expected {
        t.Error("peer fingerprint does not match certificate public key")
    }
}

func TestNewPeerIdentity_InvalidInput(t *testing.T) {
    identity, err := GenerateIdentity()
    if err != nil {
        t.Fatal(err)
    }

    cert := identity.TLSCertificate().Leaf

    tests := []struct {
        name        string
        peerID      string
        certificate *x509.Certificate
    }{
        {
            name:        "empty peer ID",
            peerID:      "",
            certificate: cert,
        },
        {
            name:        "whitespace peer ID",
            peerID:      "   ",
            certificate: cert,
        },
        {
            name:        "nil certificate",
            peerID:      "peer-123",
            certificate: nil,
        },
        {
            name:        "peer ID too long",
            peerID:      strings.Repeat("a", maxPeerIDLength+1),
            certificate: cert,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            _, err := NewPeerIdentity(tt.peerID, tt.certificate)

            if err == nil {
                t.Fatal("expected error, got nil")
            }
        })
    }
}
