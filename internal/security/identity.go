package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"time"
)

const (
	subjectName = "Airlink Node" // TODO centralize this somewhere

	serialLimitBits       = 128
	x509CertNotBefore     = -5 * time.Minute
	x509CertNotAfterYear  = 1
	x509CertNotAfterMonth = 0
	x509CertNotAfterDay   = 0
)

type Identity struct {
	certificate tls.Certificate
	fingerprint [32]byte
}

func (i *Identity) TLSCertificate() tls.Certificate {
	return i.certificate
}

func (i *Identity) Fingerprint() [32]byte {
	return i.fingerprint
}

func GenerateIdentity() (*Identity, error) {
	// key gen
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}

	// gen cert serial num
	serialLimit := new(big.Int).Lsh(big.NewInt(1), serialLimitBits)
	serialLimit.Sub(serialLimit, big.NewInt(1))

	serialNumber, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return nil, fmt.Errorf("generate serial number: %w", err)
	}

	// describe cert
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName: subjectName,
		},
		NotBefore: now.Add(x509CertNotBefore),
		NotAfter: now.AddDate(
			x509CertNotAfterYear,
			x509CertNotAfterMonth,
			x509CertNotAfterDay,
		),
		KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth,
		},
	}

	// self sign cert
	certDER, err := x509.CreateCertificate(
		rand.Reader,
		template,
		template,
		&privateKey.PublicKey,
		privateKey,
	)

	if err != nil {
		return nil, fmt.Errorf("create certificate: %w", err)
	}

	// parse cert so we can access its fields
	leaf, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, fmt.Errorf("parse certificate: %w", err)
	}

	// calculate fingerprint
	fingerprint := sha256.Sum256(leaf.RawSubjectPublicKeyInfo)

	// construct tls certificate
	tlsCert := tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  privateKey,
		Leaf:        leaf,
	}

	return &Identity{
		certificate: tlsCert,
		fingerprint: fingerprint,
	}, nil
}
