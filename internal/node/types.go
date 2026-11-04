package node

import (
	"airlink/internal/transfer"
	"context"
	"crypto/tls"
	"io"
	"net"

	"github.com/quic-go/quic-go"
)


type Stream interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
	Close() error
}

type Conn interface {
	AcceptStream(context.Context) (Stream, error)
	OpenStream() (Stream, error)
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
