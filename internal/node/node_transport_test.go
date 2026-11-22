package node

import (
	"airlink/internal/transport"
	"context"
	"crypto/tls"
	"net"

	"github.com/quic-go/quic-go"
)

type mockReceiver struct {
	listenFunc  func(tlsCfg *tls.Config, quicCfg *quic.Config) error
	cleanUpFunc func()
}

func (m *mockReceiver) Listen(tlsCfg *tls.Config, quicCfg *quic.Config) error {
	return m.listenFunc(tlsCfg, quicCfg)
}

func (m *mockReceiver) CleanUp() {
	m.cleanUpFunc()
}

type mockListener struct {
	acceptFunc func(context.Context) (transport.Conn, error)
	closeFunc  func() error
}

func (l *mockListener) Accept(ctx context.Context) (transport.Conn, error) {
	return l.acceptFunc(ctx)
}

func (l *mockListener) Close() error {
	return l.closeFunc()
}

func mockListen(network string, address *net.UDPAddr) (*net.UDPConn, error) {
	return &net.UDPConn{}, nil
}
