package comms

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"

	"github.com/quic-go/quic-go"
)

func toUDPAddress(ip string, port int) (*net.UDPAddr, error) {
	addr := net.JoinHostPort(ip, fmt.Sprintf("%d", port))
	return net.ResolveUDPAddr("udp", addr)
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
