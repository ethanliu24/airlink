package transfer

import (
	"airlink/internal/file"
	"airlink/internal/transfer/models"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"

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

type transferSend struct {
	state      sendState
	request    *models.TransferRequest
	response   *models.TransferResponse
	reader     file.Reader
	stream     Stream
	senderAddr net.Addr
}

type sendResult struct {
	Accepted     bool
	RejectReason *string
	Err          error
}

// This should run synchronously
// This function should only be called once per TransferSend
// Assumes transferSend is properly initialized
func (s *transferSend) run() *sendResult {
	var accepted bool
	var rejectReason *string
	var sendErr error

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
				sendErr = err
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

func (s *transferSend) initRequest() (*models.TransferRequest, error) {
	if s.state != sendInit {
		return nil, fmt.Errorf(
			"transferSend state is not %s in initRequest, got: %s",
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

	return &models.TransferRequest{
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

func (s *transferSend) detectMIME() (string, error) {
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

func (s *transferSend) sendTransferRequest() error {
	if s.state != sendRequest {
		return fmt.Errorf(
			"transferSend state is not %s in sendTransferRequest, got: %s",
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

func (s *transferSend) recieveTransferResponse() error {
	if s.state != sendWaitingResponse {
		return fmt.Errorf(
			"transferSend state is not %s in recieveTransferResponse, got: %s",
			sendWaitingResponse,
			s.state,
		)
	}

	if s.response == nil {
		return fmt.Errorf("transferSend response not initialized in recieveTransferResponse")
	}

	err := readMessage(s.stream, s.response)
	if err != nil {
		return err
	}

	return nil
}

func (s *transferSend) sendData() error {
	if s.state != sendSending {
		return fmt.Errorf(
			"transferSend state is not %s in sendData, got: %s",
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
func newTransferSend(reader file.Reader, stream Stream, senderAddr net.Addr) *transferSend {
	return &transferSend{
		state:      sendInit,
		response:   &models.TransferResponse{},
		reader:     reader,
		stream:     stream,
		senderAddr: senderAddr,
	}
}
