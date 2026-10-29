package transfer

import (
	"airlink/internal/transfer/models"
	"fmt"
)

type receiveState string

const (
	receiveInit              receiveState = "init"
	receiveIngestingRequest  receiveState = "ingestingRequest"
	receiveProcessingRequest receiveState = "processingRequest"
	receiveRespond           receiveState = "respond"
	receiveReceiving         receiveState = "receiving"
	receiveDone              receiveState = "done"
	receiveError             receiveState = "error"
)

type transferReceive struct {
	state    receiveState
	request  *models.TransferRequest
	response *models.TransferResponse
	stream   Stream
	consumed bool
}

type receiveResult struct {
	Request         *models.TransferRequest
	Response        *models.TransferResponse
	DestinationPath *string // abs path
	Err             error
}

// This should run synchronously
// This function should only be called once per TransferSend
// Assumes transferSend is properly initialized
func (r *transferReceive) run() *receiveResult {
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
			r.state = receiveProcessingRequest
		case receiveProcessingRequest:
			response, err := r.processResponse()
			if err != nil {
				receiveErr = err
				r.state = receiveError
				continue outer
			}

			r.response = response
			r.state = receiveRespond
		case receiveRespond:
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
		Response:        r.response,
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

// TODO always accept for now, figure out how to get data from user
func (r *transferReceive) processResponse() (*models.TransferResponse, error) {
	return &models.TransferResponse{
		Accepted: true,
		RejectReason: nil,
	}, nil
}
