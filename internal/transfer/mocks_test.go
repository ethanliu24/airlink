package comms

import (
	"airlink/internal/file"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"time"

	"github.com/quic-go/quic-go"
)

func toUDPAddress(ip string, port int) (*net.UDPAddr, error) {
	addr := net.JoinHostPort(ip, fmt.Sprintf("%d", port))
	return net.ResolveUDPAddr("udp", addr)
}

// unsecure config
func generateTLSConfig() *tls.Config {
	// Generate a fast P-256 elliptic curve key
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)

	// Strip down to the absolute bare minimum template properties
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(24 * time.Hour),
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
		NextProtos:         []string{"airlink"},
	}
}

func getQuicConfig() *quic.Config {
	return &quic.Config{
		MaxIdleTimeout:  30 * time.Second,
		KeepAlivePeriod: 15 * time.Second,
	}
}

func mockDialErr(_ context.Context, _ net.Addr, _ *tls.Config, _ *quic.Config) (Conn, error) {
	return nil, errors.New("mock dial connection failed")
}

type mockStream struct {
	writeFunc func([]byte) (int, error)
	readFunc  func([]byte) (int, error)
	closeFunc func() error
}

func (s *mockStream) Read(data []byte) (int, error) {
	return s.readFunc(data)
}

func (s *mockStream) Write(data []byte) (int, error) {
	return s.writeFunc(data)
}

func (s *mockStream) Close() error {
	return s.closeFunc()
}

type mockConn struct {
	acceptStreamFunc func(context.Context) (Stream, error)
	openStreamFunc   func() (Stream, error)
	closeWithErrFunc func(quic.ApplicationErrorCode, string) error
}

func (c *mockConn) AcceptStream(ctx context.Context) (Stream, error) {
	return c.acceptStreamFunc(ctx)
}

func (c *mockConn) OpenStream() (Stream, error) {
	return c.openStreamFunc()
}

func (c *mockConn) CloseWithError(
	code quic.ApplicationErrorCode,
	msg string,
) error {
	return c.closeWithErrFunc(code, msg)
}

type mockListener struct {
	acceptFunc func(context.Context) (Conn, error)
	closeFunc  func() error
}

func (l *mockListener) Accept(ctx context.Context) (Conn, error) {
	return l.acceptFunc(ctx)
}

func (l *mockListener) Close() error {
	return l.closeFunc()
}

type mockReader struct {

}

func (r *mockReader) Read(p []byte) (int, error) {
	return 0, nil
}

func (r *mockReader) Close() error {
	return nil
}

func newMockReader(data string) (file.Reader, error) {
	return &mockReader{

	}, nil
}
