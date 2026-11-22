package transport

import (
	"airlink/internal/transfer"
	"context"

	"github.com/quic-go/quic-go"
)

type mockStream struct {
	writeFunc func([]byte) (int, error)
	readFunc  func([]byte) (int, error)
	closeFunc func() error
	abortFunc func()
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

func (s *mockStream) Abort() {
	s.abortFunc()
}

type mockConn struct {
	acceptStreamFunc func(context.Context) (transfer.TransferStream, error)
	openStreamFunc   func() (transfer.TransferStream, error)
	closeWithErrFunc func(quic.ApplicationErrorCode, string) error
}

func (c *mockConn) AcceptStream(ctx context.Context) (transfer.TransferStream, error) {
	return c.acceptStreamFunc(ctx)
}

func (c *mockConn) OpenStream() (transfer.TransferStream, error) {
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
