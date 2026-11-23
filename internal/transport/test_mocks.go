package transport

import (
	"airlink/internal/transfer"
	"context"
	"crypto/tls"
	"time"

	"github.com/quic-go/quic-go"
)

type mockStream struct {
	writeFunc       func([]byte) (int, error)
	readFunc        func([]byte) (int, error)
	closeFunc       func() error
	abortFunc       func()
	setDeadlineFunc func(time.Time) error
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

func (s *mockStream) SetDeadline(time time.Time) error {
	return s.setDeadlineFunc(time)
}

type mockStreamState struct {
	closed      bool
	deadlineSet bool
	deadline    time.Time
}

type mockConn struct {
	acceptStreamFunc    func(context.Context) (transfer.TransferStream, error)
	openStreamFunc      func() (transfer.TransferStream, error)
	closeWithErrFunc    func(quic.ApplicationErrorCode, string) error
	openStreamSyncFunc  func(context.Context) (transfer.TransferStream, error)
	connectionStateFunc func() tls.ConnectionState
}

func (c *mockConn) AcceptStream(ctx context.Context) (transfer.TransferStream, error) {
	return c.acceptStreamFunc(ctx)
}

func (c *mockConn) OpenStream() (transfer.TransferStream, error) {
	return c.openStreamFunc()
}

func (c *mockConn) CloseWithError(code quic.ApplicationErrorCode, msg string) error {
	if c.closeWithErrFunc == nil {
        return nil
    }

	return c.closeWithErrFunc(code, msg)
}

func (c *mockConn) OpenStreamSync(ctx context.Context) (transfer.TransferStream, error) {
	return c.openStreamSyncFunc(ctx)
}

func (c *mockConn) ConnectionState() tls.ConnectionState {
	return c.connectionStateFunc()
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
