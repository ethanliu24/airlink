package transfer

import (
	"airlink/internal/event"
	"io"
	"time"

	"github.com/google/uuid"
)

const (
	transferAcceptTimeoutDuration = 5 * time.Minute
	senderName                    = "Bob"     // TODO handle this more properly
	clientName                    = "airlink" // TODO handle this more properly
	clientVersion                 = "0.0.1"   // TODO handle this more properly
)

type TransferId = string

type TransferStream interface {
	io.ReadWriteCloser
	Abort()
}

type TransferResult interface {
	isTransferResult()
}

type transfer interface {
	getId() TransferId
	setId(TransferId)
	run() TransferResult
	abortStream()
}

type generateIdTransferFunc func() (TransferId, error)

func generateTransferId() (TransferId, error) {
	uuid, err := uuid.NewV7()
	if err != nil {
		return "", err
	}

	return uuid.String(), nil
}

func emitTimeoutEvent(emitCb event.EmitEventFunc, transferId TransferId, message string) error {
	payload := &TransferTimedOutPayload{TransferId: transferId, Message: message}
	event, err := event.NewEvent(TransferTimedOutEvtName, payload)
	if err != nil {
		return err
	}

	err = emitCb(event)
	if err != nil {
		return err
	}

	return nil
}
