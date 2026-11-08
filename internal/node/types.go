package node

import (
	"airlink/internal/transfer"
	"context"
	"crypto/tls"
	"net"

	"github.com/quic-go/quic-go"
)

type Conn interface {
	AcceptStream(context.Context) (transfer.TransferStream, error)
	OpenStream() (transfer.TransferStream, error)
	CloseWithError(quic.ApplicationErrorCode, string) error
}

type Listener interface {
	Accept(context.Context) (Conn, error)
	Close() error
}

type dialFunc func(context.Context, net.Addr, *tls.Config, *quic.Config) (Conn, error)

type listenFunc func(*tls.Config, *quic.Config) (Listener, error)

type sendFunc func(stream transfer.TransferStream, filename string) (transfer.TransferId, error)

type receiveFunc func(stream transfer.TransferStream) (transfer.TransferId, error)
