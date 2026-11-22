package transfer

import (
	"airlink/internal/event"
	"airlink/internal/file"
	"airlink/internal/transfer/models"
	"airlink/internal/wire"
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

	openWriter             file.OpenWriterFunc
	stream                 TransferStream
	ctx                    context.Context
	cancel                 context.CancelFunc
	streamAbortOnce        sync.Once
	approvalWaitTimeoutDur time.Duration
	destinationPath        string
	consumed               bool

	emitEvent                event.EmitEventFunc
	acceptTransfer           chan bool
	acceptOnce               sync.Once
	selectedDestination      chan string
	requestDestinationOnce   sync.Once
	handleDupDestination     chan file.WriterCommitMode
	handleDupDestinationOnce sync.Once
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
			emitTimeoutEvent(r.emitEvent, r.id, "receive timed out")
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

	err := wire.ReadMessage(r.stream, r.request)
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
		emitTimeoutEvent(r.emitEvent, r.id, "receive timed out - did not get acceptance response")
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

	err := wire.WriteMessage(r.stream, r.response)
	if err != nil {
		return err
	}

	return nil
}

func (r *receive) requestDuplicatedFileHandling(destination string) error {
	if destination == "" {
		return fmt.Errorf("requestDuplicatedFileHandling destination path is empty")
	}

	payload := &ReceiveDuplicateFileNamePayload{TransferId: r.id, DestinationPath: destination}
	event, err := event.NewEvent(ReceiveDuplicateFileNameEvtName, payload)
	if err != nil {
		return err
	}

	err = r.emitEvent(event)
	if err != nil {
		return err
	}

	return nil
}

func (r *receive) duplicatedFileHandlingDecision(mode file.WriterCommitMode) {
	r.handleDupDestinationOnce.Do(func() {
		r.handleDupDestination <- mode
		close(r.handleDupDestination)
	})
}

func (r *receive) receiveData() (totalBytes int, receiveErr error) {
	if r.state != receiveReceiving {
		return 0, fmt.Errorf(
			"receive state is not %s in receiveData, got: %s",
			receiveReceiving,
			r.state,
		)
	}

	writer, err := r.openWriter(r.destinationPath)
	if err != nil {
		return 0, err
	}

	committed := false
	defer func() {
		if !committed {
			if abortErr := writer.Abort(); abortErr != nil {
				receiveErr = errors.Join(receiveErr, abortErr)
			}
		}
	}()

	totalBytesRead := 0
	totalBytesWrote := 0
	buf := make([]byte, receiveBufferSizeBytes)

	for {
		bytesRead, readErr := r.stream.Read(buf)
		totalBytesRead += bytesRead
		if uint64(totalBytesRead) > r.request.FileSize {
			return totalBytesWrote, fmt.Errorf(
				"file size exceeded: expected at most %d bytes, received %d",
				r.request.FileSize,
				totalBytesRead,
			)
		}

		var writeErr error
		if bytesRead > 0 {
			bytesWrote, wErr := writer.Write(buf[:bytesRead])
			totalBytesWrote += bytesWrote
			writeErr = wErr
			if writeErr == nil && bytesWrote != bytesRead {
				writeErr = io.ErrShortWrite
			}
		}

		if errors.Is(readErr, io.EOF) {
			break
		}

		if writeErr != nil {
			return totalBytesWrote, writeErr
		}

		if readErr != nil {
			return totalBytesWrote, readErr
		}
	}

	// TODO add checksum check

	if uint64(totalBytesRead) != r.request.FileSize {
		return totalBytesWrote, fmt.Errorf(
			"file size mismatch: expected %d bytes, received %d",
			r.request.FileSize,
			totalBytesRead,
		)
	}

	if totalBytesWrote != totalBytesRead {
		return totalBytesWrote, fmt.Errorf(
			"received/wrote byte count mismatch: read %d bytes, wrote %d bytes",
			totalBytesRead,
			totalBytesWrote,
		)
	}

	err = writer.Close()
	if err != nil {
		return totalBytesWrote, err
	}

	err = writer.Commit(file.WriteCommitRaise)
	if err != nil {
		if !errors.Is(err, file.DuplicateFileFound) {
			return totalBytesWrote, err
		}

		err = r.requestDuplicatedFileHandling(r.destinationPath)
		if err != nil {
			return totalBytesWrote, err
		}

		var commitMode file.WriterCommitMode
		select {
		case mode, ok := <-r.handleDupDestination:
			if !ok {
				return totalBytesWrote, fmt.Errorf("duplicate file handling decision channel closed")
			}
			commitMode = mode
		case <-r.ctx.Done():
			return totalBytesWrote, r.ctx.Err()
		}

		err = writer.Commit(commitMode)
		if err != nil {
			return totalBytesWrote, err
		}
	}

	committed = true

	return totalBytesWrote, nil
}

func newReceiveTransfer(
	stream TransferStream,
	openWriter file.OpenWriterFunc,
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
		openWriter:             openWriter,
		stream:                 stream,
		consumed:               false,
		acceptTransfer:         make(chan bool, 1),
		selectedDestination:    make(chan string, 1),
		handleDupDestination:   make(chan file.WriterCommitMode, 1),
		emitEvent:              emitEvent,
	}
}
