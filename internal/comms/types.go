package comms

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/quic-go/quic-go"
)


type Stream interface {
	Write([]byte) (int, error)
	Close() error
}

type Conn interface {
	OpenStream() (Stream, error)
	CloseWithError(quic.ApplicationErrorCode, string) error
}

type DialFunc func(context.Context, net.Addr, *tls.Config, *quic.Config) (Conn, error)
