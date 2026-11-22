package transport

import (
	"airlink/internal/transfer"
	"context"

	"github.com/quic-go/quic-go"
)

type Conn interface {
	AcceptStream(context.Context) (transfer.TransferStream, error)
	OpenStream() (transfer.TransferStream, error)
	CloseWithError(quic.ApplicationErrorCode, string) error
}
