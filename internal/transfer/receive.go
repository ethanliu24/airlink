package transfer

import (
	"airlink/internal/event"
	"airlink/internal/transfer/models"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"
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

const (
	receiveBufferSizeBytes = 1024 * 64
)

type receive struct {
	id        TransferId
	setIdOnce sync.Once

	state    receiveState
	request  *models.TransferRequest
	response *models.TransferResponse

	stream                 TransferStream
	ctx                    context.Context
	cancel                 context.CancelFunc
	streamAbortOnce        sync.Once
	approvalWaitTimeoutDur time.Duration
	destinationPath        string
	consumed               bool

	emitEvent              event.EmitEventFunc
	acceptTransfer         chan bool
	acceptOnce             sync.Once
	selectedDestination    chan string
	requestDestinationOnce sync.Once
}

type receiveResult struct {
	Request         *models.TransferRequest
	Response        *models.TransferResponse
	DestinationPath string // abs path
	Err             error
}

func (s *receiveResult) isTransferResult() {}

func (r *receive) getId() TransferId {
	return r.id
}

func (r *receive) setId(id TransferId) {
	r.setIdOnce.Do(func() { r.id = id })
}

func (r *receive) abortStream() {
	r.streamAbortOnce.Do(func() {
		r.stream.Abort()
	})
}

func (r *receive) run() TransferResult {
	var receiveErr error

	if r.getId() == "" {
		receiveErr = fmt.Errorf("transfer receive cannot start, id not set - call setId(id) to set")
		r.state = receiveError
	} else if r.consumed {
		receiveErr = fmt.Errorf("<run> already called on a receive")
		r.state = receiveError
	}

	if !r.consumed {
		r.consumed = true
		abortDone := make(chan struct{})
		stopAbort := context.AfterFunc(r.ctx, func() {
			defer close(abortDone)
			r.abortStream()
		})

		defer func() {
			if !stopAbort() {
				<-abortDone
			}

			_ = r.stream.Close()
		}()
	}

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
			response, destinationPath, err := r.processResponse()
			if err != nil {
				receiveErr = err
				r.state = receiveError
				continue outer
			}

			r.response = response
			r.destinationPath = destinationPath
			r.state = receiveRespond
		case receiveRespond:
			err := r.sendResponse()
			if err != nil {
				receiveErr = err
				r.state = receiveError
				continue outer
			}

			if r.response.Accepted {
				r.state = receiveReceiving
			} else {
				r.state = receiveDone
			}
		case receiveReceiving:
			_, err := r.receiveData()
			if err != nil {
				if ctxErr := r.ctx.Err(); ctxErr != nil {
					receiveErr = ctxErr
				} else {
					receiveErr = err
				}

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
		DestinationPath: r.destinationPath,
		Err:             receiveErr,
	}
}

func (r *receive) ingestRequst() error {
	if r.state != receiveIngestingRequest {
		return fmt.Errorf(
			"receive state is not %s in ingestRequst, got: %s",
			receiveIngestingRequest,
			r.state,
		)
	}

	if r.request == nil {
		return fmt.Errorf("receive request nil - not initialized properly")
	}

	err := readMessage(r.stream, r.request)
	if err != nil {
		return err
	}

	return nil
}

func (r *receive) requestUserTransferAcceptance(transferId TransferId) error {
	payload := &RequestTransferAcceptancePayload{TransferId: transferId}
	event, err := event.NewEvent(RequestTransferAcceptanceEvtName, payload)
	if err != nil {
		return err
	}

	err = r.emitEvent(event)
	if err != nil {
		return err
	}

	return nil
}

func (r *receive) userTransferAcceptanceDecision(accepted bool) {
	r.acceptOnce.Do(func() {
		r.acceptTransfer <- accepted
		close(r.acceptTransfer)
	})
}

func (r *receive) requestDownloadDestination(transferId TransferId) error {
	payload := &RequestDownloadDestinationPayload{TransferId: transferId}
	event, err := event.NewEvent(RequestDownloadDestinationEvtName, payload)
	if err != nil {
		return err
	}

	err = r.emitEvent(event)
	if err != nil {
		return err
	}

	return nil
}

func (r *receive) userDownloadDestinationDecision(path string) error {
	if path == "" {
		return fmt.Errorf("userDownloadDestinationDecision destination path is empty")
	}

	if !filepath.IsAbs(path) {
		return fmt.Errorf("userDownloadDestinationDecision destination path must be absolute")
	}

	pathAbs := filepath.Clean(path)

	r.requestDestinationOnce.Do(func() {
		r.selectedDestination <- pathAbs
		close(r.selectedDestination)
	})

	return nil
}

func (r *receive) processResponse() (*models.TransferResponse, string, error) {
	if r.state != receiveProcessingRequest {
		return nil, "", fmt.Errorf(
			"receive state is not %s in processResponse, got: %s",
			receiveProcessingRequest,
			r.state,
		)
	}

	approvalCtx, cancel := context.WithTimeout(r.ctx, r.approvalWaitTimeoutDur)
	defer cancel()

	abortDone := make(chan struct{})
	stopAbort := context.AfterFunc(approvalCtx, func() {
		defer close(abortDone)
		r.stream.Abort()
	})

	abortStopped := false
	stopAbortAndWait := func() error {
		if !abortStopped {
			abortStopped = true

			if !stopAbort() {
				<-abortDone
			}
		}

		return approvalCtx.Err()
	}

	defer stopAbortAndWait()

	if err := approvalCtx.Err(); err != nil {
		return nil, "", fmt.Errorf("waiting for transfer approval: %w", err)
	}

	err := r.requestUserTransferAcceptance(r.getId())
	if err != nil {
		return nil, "", err
	}

	var accepted bool
	select {
	case accepted = <-r.acceptTransfer:
	case <-approvalCtx.Done():
		return nil, "", fmt.Errorf(
			"waiting for transfer acceptance: %w",
			approvalCtx.Err(),
		)
	}

	if !accepted {
		if err := stopAbortAndWait(); err != nil {
			return nil, "", fmt.Errorf("waiting for transfer acceptance: %w", err)
		}

		return &models.TransferResponse{
			Id:           r.request.Id,
			Accepted:     false,
			RejectReason: nil,
		}, "", nil
	}

	err = r.requestDownloadDestination(r.getId())
	if err != nil {
		return nil, "", err
	}

	var destination string
	select {
	case destination = <-r.selectedDestination:
	case <-approvalCtx.Done():
		return nil, "", fmt.Errorf(
			"waiting for download destination: %w",
			approvalCtx.Err(),
		)
	}

	if err := stopAbortAndWait(); err != nil {
		return nil, "", fmt.Errorf("waiting for download destination: %w", err)
	}

	return &models.TransferResponse{
		Id:           r.request.Id,
		Accepted:     accepted,
		RejectReason: nil,
	}, destination, nil
}

func (r *receive) sendResponse() error {
	if r.state != receiveRespond {
		return fmt.Errorf(
			"receive state is not %s in sendResponse, got: %s",
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

func (r *receive) receiveData() (int, error) {
	if r.state != receiveReceiving {
		return 0, fmt.Errorf(
			"receive state is not %s in receiveData, got: %s",
			receiveReceiving,
			r.state,
		)
	}

	totalBytesRead := 0
	buf := make([]byte, receiveBufferSizeBytes)

	for {
		n, err := r.stream.Read(buf)
		totalBytesRead += n

		// TODO stream the file in destination
		fmt.Printf("received in %s: %s", r.destinationPath, string(buf[:n]))

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return totalBytesRead, err
		}
	}

	return totalBytesRead, nil
}

func newReceiveTransfer(
	stream TransferStream,
	emitEvent event.EmitEventFunc,
	ctx context.Context,
	cancel context.CancelFunc,
	approvalWaitTimeoutDur time.Duration,
) *receive {
	return &receive{
		id:                     "",
		state:                  receiveInit,
		request:                &models.TransferRequest{},
		ctx:                    ctx,
		cancel:                 cancel,
		approvalWaitTimeoutDur: approvalWaitTimeoutDur,
		stream:                 stream,
		consumed:               false,
		acceptTransfer:         make(chan bool, 1),
		selectedDestination:    make(chan string, 1),
		emitEvent:              emitEvent,
	}
}
