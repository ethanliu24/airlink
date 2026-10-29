package transfer

import (
	"airlink/internal/transfer/models"
	"fmt"
)

type receiveState string

const (
	receiveInit             receiveState = "init"
	receiveInvestingRequest receiveState = "ingestingRequest"
	receiveProcess          receiveState = "process"
	receiveRespond          receiveState = "respond"
	receiveReceiving        receiveState = "receiving"
	receiveDone             receiveState = "done"
	receiveError            receiveState = "error"
)

type transferReceive struct {
	state    receiveState
	stream   Stream
	consumed bool
}

type receiveResult struct {
    Request         *models.TransferRequest
    Accepted        bool
    RejectReason    *string
    DestinationPath *string  // abs path
    Err             error
}

func (r *transferReceive) run() *receiveResult {
	var request *models.TransferRequest
	var accepted bool
	var rejectReason *string
	var destinationPath *string
	var receiveErr error

	if r.consumed {
		receiveErr = fmt.Errorf("<run> already called on a transferSend")
		r.state = receiveError
	}

	r.consumed = true

outer:
	for {
		switch r.state {
		case receiveInit:
			r.state = receiveInvestingRequest
		case receiveInvestingRequest:
			break outer
		case receiveDone, receiveError:
			break outer
		}
	}

	return &receiveResult{
		Request: request,
		Accepted: accepted,
		RejectReason: rejectReason,
		DestinationPath: destinationPath,
		Err: receiveErr,
	}
}
