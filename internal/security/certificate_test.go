
package security

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"strings"
	"testing"
	"time"
)

func cloneCertificateForTest(cert *x509.Certificate) *x509.Certificate {
	cloned := *cert

	// Avoid modifying the original certificate's signature.
	cloned.Signature = append([]byte(nil), cert.Signature...)

	return &cloned
}

func TestValidateCertificate_Valid(t *testing.T) {
	identity, err := GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity() error = %v", err)
	}

	cert := identity.TLSCertificate().Leaf

	if err := ValidateCertificate(cert); err != nil {
		t.Fatalf("ValidateCertificate() error = %v", err)
	}
}

func TestValidateCertificate_Nil(t *testing.T) {
	err := ValidateCertificate(nil)

	if err == nil {
		t.Fatal("expected error for nil certificate")
	}
}

func TestValidateCertificate_InvalidCertificates(t *testing.T) {
	identity, err := GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity() error = %v", err)
	}

	base := identity.TLSCertificate().Leaf

	tests := []struct {
		name    string
		wantErr string
		mutate  func(*x509.Certificate)
	}{
		{
			name:    "invalid version",
			wantErr: "unsupported X.509 version",
			mutate: func(cert *x509.Certificate) {
				cert.Version = 2
			},
		},
		{
			name:    "nil serial number",
			wantErr: "invalid or non positive serial number",
			mutate: func(cert *x509.Certificate) {
				cert.SerialNumber = nil
			},
		},
		{
			name:    "zero serial number",
			wantErr: "invalid or non positive serial number",
			mutate: func(cert *x509.Certificate) {
				cert.SerialNumber = big.NewInt(0)
			},
		},
		{
			name:    "negative serial number",
			wantErr: "invalid or non positive serial number",
			mutate: func(cert *x509.Certificate) {
				cert.SerialNumber = big.NewInt(-1)
			},
		},
		{
			name:    "missing subject and SANs",
			wantErr: "missing subject and Subject Alternative Names",
			mutate: func(cert *x509.Certificate) {
				cert.Subject = pkix.Name{}
				cert.DNSNames = nil
				cert.IPAddresses = nil
			},
		},
		{
			name:    "missing issuer",
			wantErr: "certificate missing issuer",
			mutate: func(cert *x509.Certificate) {
				cert.Issuer = pkix.Name{}
			},
		},
		{
			name:    "not yet valid",
			wantErr: "certificate is not yet valid",
			mutate: func(cert *x509.Certificate) {
				cert.NotBefore = time.Now().Add(time.Hour)
			},
		},
		{
			name:    "expired certificate",
			wantErr: "certificate has expired",
			mutate: func(cert *x509.Certificate) {
				cert.NotAfter = time.Now().Add(-time.Hour)
			},
		},
		{
			name:    "not self-issued",
			wantErr: "certificate is not self-issued",
			mutate: func(cert *x509.Certificate) {
				cert.RawIssuer = []byte("different issuer")
			},
		},
		{
			name:    "invalid self-signature",
			wantErr: "self-signature verification failed",
			mutate: func(cert *x509.Certificate) {
				cert.Signature[0] ^= 0xff
			},
		},
		{
			name:    "unsupported public key algorithm",
			wantErr: "unsupported or weak public key algorithm",
			mutate: func(cert *x509.Certificate) {
				cert.PublicKeyAlgorithm = x509.UnknownPublicKeyAlgorithm
			},
		},
		{
			name:    "missing key usage",
			wantErr: "certificate has no KeyUsage",
			mutate: func(cert *x509.Certificate) {
				cert.KeyUsage = 0
			},
		},
		{
			name:    "CA missing certificate signing usage",
			wantErr: "CA certificate missing KeyUsageCertSign",
			mutate: func(cert *x509.Certificate) {
				cert.IsCA = true
				cert.KeyUsage = x509.KeyUsageDigitalSignature
			},
		},
		{
			name:    "leaf missing digital signature usage",
			wantErr: "leaf certificate missing KeyUsageDigitalSignature",
			mutate: func(cert *x509.Certificate) {
				cert.KeyUsage = x509.KeyUsageKeyEncipherment
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cert := cloneCertificateForTest(base)

			tt.mutate(cert)

			err := ValidateCertificate(cert)

			if err == nil {
				t.Fatal("expected validation error, got nil")
			}

			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf(
					"expected error containing %q, got %q",
					tt.wantErr,
					err.Error(),
				)
			}
		})
	}
}
