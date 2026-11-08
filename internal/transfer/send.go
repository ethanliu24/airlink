package transfer

import (
	"airlink/internal/file"
	"airlink/internal/transfer/models"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type sendState string

const (
	sendInit            sendState = "init"
	sendRequest         sendState = "request"
	sendWaitingResponse sendState = "waitingResponse"
	sendRejected        sendState = "rejected"
	sendSending         sendState = "sending"
	sendDone            sendState = "done"
	sendError           sendState = "error"
)

const (
	mimeDetectionBufferSizeBytes = 512
	fileReadChunkSizeBytes       = 1024 * 64
)

type send struct {
	id        TransferId
	setIdOnce sync.Once

	state    sendState
	request  *models.TransferRequest
	response *models.TransferResponse

	reader          file.Reader
	stream          TransferStream
	streamAbortOnce sync.Once
	ctx             context.Context

	senderAddr           net.Addr
	acceptWaitTimeoutDur time.Duration
	consumed             bool
}

type sendResult struct {
	Accepted     bool
	RejectReason *string
	Err          error
}

func (s *sendResult) isTransferResult() {}

func (s *send) getId() TransferId {
	return s.id
}

func (s *send) setId(id TransferId) {
	s.setIdOnce.Do(func() { s.id = id })
}

func (s *send) abortStream() {
    s.streamAbortOnce.Do(func() {
        s.stream.Abort()
    })
}

func (s *send) run() TransferResult {
	var accepted bool
	var rejectReason *string
	var sendErr error

	if s.getId() == "" {
		sendErr = fmt.Errorf("transfer send cannot start, id not set - call setId(id) to set")
		s.state = sendError
	} else if s.consumed {
		sendErr = fmt.Errorf("<run> already called on a send")
		s.state = sendError
	}

	if !s.consumed {
		s.consumed = true
		abortDone := make(chan struct{})
		stopAbort := context.AfterFunc(s.ctx, func() {
			defer close(abortDone)
			s.abortStream()
		})

		defer func() {
			if !stopAbort() {
				<-abortDone
			}

			_ = s.reader.Close()
			_ = s.stream.Close()
		}()
	}

outer:
	for {
		switch s.state {
		case sendInit:
			request, err := s.initRequest()
			if err != nil {
				sendErr = err
				s.state = sendError
				continue outer
			}

			s.request = request
			s.state = sendRequest
		case sendRequest:
			err := s.sendTransferRequest()
			if err != nil {
				sendErr = err
				s.state = sendError
				continue outer
			}

			s.state = sendWaitingResponse
		case sendWaitingResponse:
			err := s.recieveTransferResponse()
			if err != nil {
				sendErr = err
				s.state = sendError
				continue outer
			}

			if s.response.Accepted {
				accepted = true
				s.state = sendSending
			} else {
				accepted = false
				rejectReason = s.response.RejectReason
				s.state = sendRejected
			}
		case sendSending:
			err := s.sendData()
			if err != nil {
				if ctxErr := s.ctx.Err(); ctxErr != nil {
					sendErr = ctxErr
				} else {
					sendErr = err
				}

				s.state = sendError
				continue outer
			}

			s.state = sendDone
		case sendDone, sendRejected, sendError:
			break outer
		default:
			sendErr = fmt.Errorf("invalid transfer send state: %q", s.state)
			s.state = sendError
			continue outer
		}
	}

	return &sendResult{
		Accepted:     accepted,
		RejectReason: rejectReason,
		Err:          sendErr,
	}
}

func (s *send) initRequest() (*models.TransferRequest, error) {
	if s.state != sendInit {
		return nil, fmt.Errorf(
			"send state is not %s in initRequest, got: %s",
			sendInit,
			s.state,
		)
	}

	stat, err := s.reader.Stat()
	if err != nil {
		return nil, err
	}

	mimeType, err := s.detectMIME()
	if err != nil {
		return nil, err
	}

	host, _, err := net.SplitHostPort(s.senderAddr.String())
	if err != nil {
		return nil, err
	}

	uuid, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}

	return &models.TransferRequest{
		Id:            uuid.String(),
		Filename:      stat.Name(),
		Extension:     filepath.Ext(stat.Name()),
		FileSize:      uint64(stat.Size()),
		CreatedAt:     timestamppb.New(stat.ModTime()), // omit CreatedAt until you implement platform-specific retrieval
		ModifiedAt:    timestamppb.New(stat.ModTime()),
		Mime:          mimeType,
		SenderIp:      host,
		SenderName:    senderName,
		ClientName:    clientName,
		ClientVersion: clientVersion,
	}, nil
}

func (s *send) detectMIME() (string, error) {
	buf := make([]byte, mimeDetectionBufferSizeBytes)

	n, err := io.ReadFull(s.reader, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return "", err
	}

	_, seekErr := s.reader.Seek(0, io.SeekStart)
	if seekErr != nil {
		return "", seekErr
	}

	if n == 0 {
		return "application/octet-stream", nil
	}

	return http.DetectContentType(buf[:n]), nil
}

func (s *send) sendTransferRequest() error {
	if s.state != sendRequest {
		return fmt.Errorf(
			"send state is not %s in sendTransferRequest, got: %s",
			sendRequest,
			s.state,
		)
	}

	err := writeMessage(s.stream, s.request)
	if err != nil {
		return err
	}

	return nil
}

func (s *send) recieveTransferResponse() error {
	if s.state != sendWaitingResponse {
		return fmt.Errorf(
			"send state is not %s in recieveTransferResponse, got: %s",
			sendWaitingResponse,
			s.state,
		)
	}

	if s.response == nil {
		return fmt.Errorf("send response not initialized in recieveTransferResponse")
	}

	approvalCtx, cancel := context.WithTimeout(s.ctx, s.acceptWaitTimeoutDur)
	defer cancel()

	abortDone := make(chan struct{})
	stopAbort := context.AfterFunc(approvalCtx, func() {
		defer close(abortDone)
		s.stream.Abort()
	})

	err := readMessage(s.stream, s.response)

	if !stopAbort() {
		<-abortDone
	}

	if approvalCtx.Err() != nil {
		return fmt.Errorf("waiting for transfer approval: %w", approvalCtx.Err())
	}

	return err
}

func (s *send) sendData() error {
	if s.state != sendSending {
		return fmt.Errorf(
			"send state is not %s in sendData, got: %s",
			sendSending,
			s.state,
		)
	}

	buf := make([]byte, fileReadChunkSizeBytes)

	_, err := io.CopyBuffer(s.stream, s.reader, buf)
	if err != nil {
		return fmt.Errorf("failed to send file data: %w", err)
	}

	return nil
}

// Assume caller will handle opening and closing the file and stream
func newSendTransfer(
	ctx context.Context,
	reader file.Reader,
	stream TransferStream,
	senderAddr net.Addr,
	acceptWaitTimeoutDur time.Duration,
) *send {
	return &send{
		state:                sendInit,
		response:             &models.TransferResponse{},
		reader:               reader,
		stream:               stream,
		ctx:                  ctx,
		senderAddr:           senderAddr,
		acceptWaitTimeoutDur: acceptWaitTimeoutDur,
		consumed:             false,
	}
}
