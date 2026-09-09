package node

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"crypto/tls"
	"encoding/pem"
	"math/big"
	"time"

	"github.com/quic-go/quic-go"
)

// TODO properly config this
func generateTLSConfig() *tls.Config {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{Organization: []string{"Local Test"}},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour * 24),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	certDER, _ := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)

	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	tlsCert, _ := tls.X509KeyPair(certPEM, keyPEM)

	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		MinVersion:   tls.VersionTLS13,
		InsecureSkipVerify: true, // TODO make this secure
		NextProtos:   []string{"airlink"},
	}
}

// TODO properly config this
func getQuicConfig() *quic.Config {
	return &quic.Config{
		// InitialStreamReceiveWindow: 1<<20, // 1 MB
		// MaxStreamReceiveWindow: 6<<20, // 6 MB
		// InitialConnectionReceiveWindow: 2<<20, // 2 MB
		// MaxConnectionReceiveWindow: 12<<20, // 12 MB
		// MaxIncomingStreams: 100, // bidirectional streams
		// MaxIncomingUniStreams: 100, // unidirectional streams
		MaxIdleTimeout: 30 * time.Second,
		KeepAlivePeriod: 15 * time.Second,
	}
}
