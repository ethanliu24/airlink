package security

import (
	"bytes"
	"crypto/x509"
	"fmt"
	"time"
)

func ValidateCertificate(certificate *x509.Certificate) error {
	if certificate == nil {
		return fmt.Errorf("certificate validation: certificate is nil")
	}

	// validate certificate fields
	if certificate.Version != 3 {
		return fmt.Errorf("unsupported X.509 version: %d (expected 3)", certificate.Version)
	}

	if certificate.SerialNumber == nil || certificate.SerialNumber.Sign() <= 0 {
		return fmt.Errorf("certificate validation: invalid or non positive serial number")
	}

	if certificate.Subject.String() == "" && len(certificate.DNSNames) == 0 && len(certificate.IPAddresses) == 0 {
		return fmt.Errorf("certificate validation: certificate missing subject and Subject Alternative Names (SANs)")
	}

	if certificate.Issuer.String() == "" {
		return fmt.Errorf("certificate validation: certificate missing issuer")
	}

	// validate certificate valid time range
	now := time.Now().UTC()
	if now.Before(certificate.NotBefore) {
		return fmt.Errorf(
			"certificate validation: certificate is not yet valid (valid after: %s)",
			certificate.NotBefore.Local().Format(time.RFC3339),
		)
	}

	if now.After(certificate.NotAfter) {
		return fmt.Errorf(
			"certificate validation: certificate has expired (expired at: %s)",
			certificate.NotAfter.Format(time.RFC3339),
		)
	}

	// validate self signed certificate signature
	if !bytes.Equal(certificate.RawIssuer, certificate.RawSubject) {
		return fmt.Errorf("certificate validation: certificate is not self-issued")
	}

	if err := certificate.CheckSignature(
		certificate.SignatureAlgorithm,
		certificate.RawTBSCertificate,
		certificate.Signature,
	); err != nil {
		return fmt.Errorf("certificate validation: self-signature verification failed: %w", err)
	}

	// validate supported public key algorithm
	switch certificate.PublicKeyAlgorithm {
	case x509.RSA, x509.ECDSA, x509.Ed25519:
	default:
		return fmt.Errorf(
			"certificate validation: unsupported or weak public key algorithm - %s",
			certificate.PublicKeyAlgorithm.String(),
		)
	}

	// validate key usage
	if certificate.KeyUsage == 0 {
		return fmt.Errorf("certificate validation: certificate has no KeyUsage extensions set")
	}

	if certificate.IsCA {
		if certificate.KeyUsage&x509.KeyUsageCertSign == 0 {
			return fmt.Errorf("certificate validation: CA certificate missing KeyUsageCertSign")
		}
	} else {
		if certificate.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
			return fmt.Errorf("certificate validation: leaf certificate missing KeyUsageDigitalSignature")
		}
	}

	return nil
}
