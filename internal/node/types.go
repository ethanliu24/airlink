package node

import (
	"airlink/internal/transfer"
	"context"
	"crypto/tls"
	"io"
	"net"

	"github.com/quic-go/quic-go"
)

type Conn interface {
	AcceptStream(context.Context) (io.ReadWriteCloser, error)
	OpenStream() (io.ReadWriteCloser, error)
	CloseWithError(quic.ApplicationErrorCode, string) error
}

type Listener interface {
	Accept(context.Context) (Conn, error)
	Close() error
}

type dialFunc func(context.Context, net.Addr, *tls.Config, *quic.Config) (Conn, error)

type listenFunc func(*tls.Config, *quic.Config) (Listener, error)

type sendFunc func(stream io.ReadWriteCloser, filename string) (transfer.TransferId, error)

type receiveFunc func(stream io.ReadWriteCloser) (transfer.TransferId, error)
