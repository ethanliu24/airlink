package transfer

import (
	"airlink/internal/transfer/models"
	"errors"
	"fmt"
	"io"
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
// This function should only be called once per transferReceive
// Assumes transferReceive is properly initialized
func (r *transferReceive) run() *receiveResult {
	var destinationPath *string
	var receiveErr error

	if r.consumed {
		receiveErr = fmt.Errorf("<run> already called on a transferReceive")
		r.state = receiveError
	}

	r.consumed = true

outer:
	for {
		switch r.state {
		case receiveInit:
			r.state = receiveIngestingRequest
		case receiveIngestingRequest:
			err := r.ingestRequst()
			if err != nil {
				receiveErr = err
				r.state = receiveError
				continue outer
			}

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
			err := r.sendResponse()
			if err != nil {
				receiveErr = err
				r.state = receiveError
				continue outer
			}

			r.state = receiveReceiving
		case receiveReceiving:
			_, err := r.receiveData()
			if err != nil {
				receiveErr = err
				r.state = receiveError
				continue outer
			}

			r.state = receiveDone
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

func (r *transferReceive) ingestRequst() error {
	if r.state != receiveIngestingRequest {
		return fmt.Errorf(
			"transferReceive state is not %s in ingestRequst, got: %s",
			receiveIngestingRequest,
			r.state,
		)
	}

	if r.request == nil {
		return fmt.Errorf("transferReceive request nil - not initialized properly")
	}

	err := readMessage(r.stream, r.request)
	if err != nil {
		return err
	}

	return nil
}

// TODO always accept for now, figure out how to get data from user
// TODO return detination too
func (r *transferReceive) processResponse() (*models.TransferResponse, error) {
	if r.state != receiveProcessingRequest {
		return nil, fmt.Errorf(
			"transferReceive state is not %s in processResponse, got: %s",
			receiveProcessingRequest,
			r.state,
		)
	}

	return &models.TransferResponse{
		Accepted: true,
		RejectReason: nil,
	}, nil
}

func (r *transferReceive) sendResponse() error {
	if r.state != receiveRespond {
		return fmt.Errorf(
			"transferReceive state is not %s in sendResponse, got: %s",
			receiveRespond,
			r.state,
		)
	}

	err := writeMessage(r.stream, r.response)
	if err != nil {
		return err
	}

	return nil
}

func (r *transferReceive) receiveData() (int, error) {
	if r.state != receiveReceiving {
		return 0, fmt.Errorf(
			"transferReceive state is not %s in receiveData, got: %s",
			receiveReceiving,
			r.state,
		)
	}

	totalBytesRead := 0
	buf := make([]byte, RECEIVER_BUFFER_SIZE_BYTES)

	for {
		n, err := r.stream.Read(buf)
		totalBytesRead += n

		// TODO stream the file in destination
		fmt.Printf("%s", string(buf[:n]))

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return totalBytesRead, err
		}
	}

	return totalBytesRead, nil
}
