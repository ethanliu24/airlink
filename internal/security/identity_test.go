
package security

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/x509"
	"testing"
	"time"
)

func TestGenerateIdentity(t *testing.T) {
	identity, err := GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity() error = %v", err)
	}

	if identity == nil {
		t.Fatal("GenerateIdentity() returned nil identity")
	}

	cert := identity.TLSCertificate()

	// Check TLS certificate.
	if len(cert.Certificate) != 1 {
		t.Fatalf("expected 1 certificate, got %d", len(cert.Certificate))
	}

	if cert.PrivateKey == nil {
		t.Fatal("expected private key, got nil")
	}

	if cert.Leaf == nil {
		t.Fatal("expected parsed leaf certificate, got nil")
	}

	// Check private key type.
	privateKey, ok := cert.PrivateKey.(*ecdsa.PrivateKey)
	if !ok {
		t.Fatalf("expected ECDSA private key, got %T", cert.PrivateKey)
	}

	// Check certificate.
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("failed to parse certificate: %v", err)
	}

	if !bytes.Equal(leaf.Raw, cert.Leaf.Raw) {
		t.Fatal("TLS certificate leaf does not match certificate DER")
	}

	if leaf.SerialNumber.Sign() <= 0 {
		t.Fatal("expected positive certificate serial number")
	}

	if leaf.Subject.CommonName != subjectName {
		t.Errorf("expected subject name %q, got %q",
			subjectName, leaf.Subject.CommonName)
	}

	// Check public key matches private key.
	publicKey, ok := leaf.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		t.Fatalf("expected ECDSA public key, got %T", leaf.PublicKey)
	}

	if publicKey.X.Cmp(privateKey.X) != 0 ||
		publicKey.Y.Cmp(privateKey.Y) != 0 {
		t.Fatal("certificate public key does not match private key")
	}

	// Check certificate validity.
	now := time.Now()

	if leaf.NotBefore.After(now) {
		t.Error("certificate is not valid yet")
	}

	if !leaf.NotAfter.After(now) {
		t.Error("certificate has already expired")
	}

	// Check key usage.
	if leaf.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
		t.Error("expected DigitalSignature key usage")
	}

	hasServerAuth := false
	hasClientAuth := false

	for _, usage := range leaf.ExtKeyUsage {
		switch usage {
		case x509.ExtKeyUsageServerAuth:
			hasServerAuth = true
		case x509.ExtKeyUsageClientAuth:
			hasClientAuth = true
		}
	}

	if !hasServerAuth {
		t.Error("expected ServerAuth extended key usage")
	}

	if !hasClientAuth {
		t.Error("expected ClientAuth extended key usage")
	}

	// Verify self-signature.
	err = leaf.CheckSignature(
		leaf.SignatureAlgorithm,
		leaf.RawTBSCertificate,
		leaf.Signature,
	)
	if err != nil {
		t.Errorf("certificate self-signature verification failed: %v", err)
	}
}

func TestIdentityFingerprint(t *testing.T) {
	identity, err := GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity() error = %v", err)
	}

	leaf := identity.TLSCertificate().Leaf

	// Check fingerprint against expected hash.
	expected := sha256.Sum256(leaf.RawSubjectPublicKeyInfo)

	if got := identity.Fingerprint(); got != expected {
		t.Errorf("fingerprint mismatch: got %x, want %x", got, expected)
	}

	// Fingerprint should remain stable.
	first := identity.Fingerprint()
	second := identity.Fingerprint()

	if first != second {
		t.Error("fingerprint changed between calls")
	}
}

func TestGenerateIdentityUnique(t *testing.T) {
	identity1, err := GenerateIdentity()
	if err != nil {
		t.Fatalf("first GenerateIdentity() error = %v", err)
	}

	identity2, err := GenerateIdentity()
	if err != nil {
		t.Fatalf("second GenerateIdentity() error = %v", err)
	}

	if identity1.Fingerprint() == identity2.Fingerprint() {
		t.Error("expected different fingerprints for different identities")
	}
}
