package transfer

import (
	"airlink/internal/transfer/models"
	"fmt"
)

type receiveState string

const (
	receiveInit             receiveState = "init"
	receiveIngestingRequest receiveState = "ingestingRequest"
	receiveProcessRequest   receiveState = "processRequest"
	receiveRespond          receiveState = "respond"
	receiveReceiving        receiveState = "receiving"
	receiveDone             receiveState = "done"
	receiveError            receiveState = "error"
)

type transferReceive struct {
	state    receiveState
	request  *models.TransferRequest
	stream   Stream
	consumed bool
}

type receiveResult struct {
	Request         *models.TransferRequest
	Accepted        bool
	RejectReason    *string
	DestinationPath *string // abs path
	Err             error
}

// This should run synchronously
// This function should only be called once per TransferSend
// Assumes transferSend is properly initialized
func (r *transferReceive) run() *receiveResult {
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
			r.state = receiveIngestingRequest
		case receiveIngestingRequest:
			request, err := r.ingestRequst()
			if err != nil {
				receiveErr = err
				r.state = receiveError
				continue outer
			}

			r.request = request
			r.state = receiveProcessRequest
		case receiveProcessRequest:
			break outer
		case receiveDone, receiveError:
			break outer
		default:
			receiveErr = fmt.Errorf("invalid transfer receive state: %q", r.state)
			r.state = receiveError
			continue outer
		}
	}

	return &receiveResult{
		Request:         r.request,
		Accepted:        accepted,
		RejectReason:    rejectReason,
		DestinationPath: destinationPath,
		Err:             receiveErr,
	}
}

func (r *transferReceive) ingestRequst() (*models.TransferRequest, error) {
	if r.state != receiveIngestingRequest {
		return nil, fmt.Errorf(
			"transferReceive state is not %s in ingestRequst, got: %s",
			receiveIngestingRequest,
			r.state,
		)
	}

	var request *models.TransferRequest = &models.TransferRequest{}

	err := readMessage(r.stream, request)
	if err != nil {
		return nil, err
	}

	return request, nil
}
