package comms

import (
	"context"
	"crypto/tls"
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
}

type DialFunc func(context.Context, net.Addr, *tls.Config, *quic.Config) (Conn, error)

type ListenFunc func(*tls.Config, *quic.Config) (Listener, error)
