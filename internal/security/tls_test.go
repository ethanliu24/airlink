package security

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"testing"
)

func mustFingerprint(publicKeyDER []byte) []byte {
	fingerprint := sha256.Sum256(publicKeyDER)
	return fingerprint[:]
}

func TestIdentityTLSConfig(t *testing.T) {
	identity, err := GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity() error = %v", err)
	}

	config := identity.TLSConfig()

	if config == nil {
		t.Fatal("TLSConfig() returned nil")
	}

	// Check TLS version.
	if config.MinVersion != tls.VersionTLS13 {
		t.Errorf("expected minimum TLS version TLS 1.3, got %x",
			config.MinVersion)
	}

	// Check ALPN.
	if len(config.NextProtos) != 1 || config.NextProtos[0] != alpnProtocol {
		t.Errorf("expected ALPN %q, got %v",
			alpnProtocol, config.NextProtos)
	}

	// Ensure certificate verification hasn't been disabled.
	if config.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify to be false")
	}

	// Check certificate.
	if len(config.Certificates) != 1 {
		t.Fatalf("expected 1 certificate, got %d",
			len(config.Certificates))
	}

	cert := config.Certificates[0]

	if len(cert.Certificate) != 1 {
		t.Fatalf("expected 1 certificate in chain, got %d",
			len(cert.Certificate))
	}

	if cert.PrivateKey == nil {
		t.Fatal("expected private key, got nil")
	}

	// Ensure TLS config uses the identity's certificate.
	expectedCert := identity.TLSCertificate()

	if !bytes.Equal(
		cert.Certificate[0],
		expectedCert.Certificate[0],
	) {
		t.Error("TLS config certificate does not match identity certificate")
	}

	// Verify the certificate can be parsed.
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("failed to parse TLS certificate: %v", err)
	}

	if leaf.Subject.CommonName != subjectName {
		t.Errorf("expected subject %q, got %q",
			subjectName, leaf.Subject.CommonName)
	}

	// Verify the certificate fingerprint matches the identity.
	fingerprint := identity.Fingerprint()
	if !bytes.Equal(
		fingerprint[:],
		mustFingerprint(leaf.RawSubjectPublicKeyInfo),
	) {
		t.Error("TLS certificate fingerprint does not match identity")
	}
}
