package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"math/big"
	"time"
)

const alpnProtocol = "airlink" // TODO centralize somewhere

var ErrPeerCertificateMissing = errors.New("peer certificate missing")

func (i *Identity) TLSConfig() *tls.Config {
	return &tls.Config{
		Certificates:       []tls.Certificate{i.certificate},
		MinVersion:         tls.VersionTLS13,
		NextProtos:         []string{alpnProtocol},
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return ErrPeerCertificateMissing
			}

			// TODO log err
			return ValidateCertificate(cs.PeerCertificates[0])
		},
	}
}

// unsecure config for test cases only
func GenerateTestTLSConfig() *tls.Config {
	// Generate a fast P-256 elliptic curve key
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)

	// Strip down to the absolute bare minimum template properties
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().UTC(),
		NotAfter:     time.Now().UTC().Add(24 * time.Hour),
	}

	certDER, _ := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	keyDER, _ := x509.MarshalECPrivateKey(key)

	// Bypass string PEM blocks and create the keypair directly from raw DER bytes
	tlsCert, _ := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	)

	return &tls.Config{
		Certificates:       []tls.Certificate{tlsCert},
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: true,
		NextProtos:         []string{alpnProtocol},
	}
}
