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
	fileReadChunkSizeBytes = 1024 * 64
)

type transferSend struct {
	state    sendState
	request  *models.TransferRequest
	response *models.TransferResponse
}

type sendResult struct {
	Accepted     bool
	RejectReason *string
	Err          error
}

// Assume caller will handle opening and closing the file and stream
// This should run synchronously
// This function should only be called once per TransferSend
func (s *transferSend) run(reader file.Reader, stream Stream, senderAddr net.Addr) *sendResult {
	// TODO make sure this function is called once max per TransferSend
	var accepted bool
	var rejectReason *string
	var sendErr error

outer:
	for {
		switch s.state {
		case sendInit:
			request, err := initRequest(reader, senderAddr)
			if err != nil {
				sendErr = err
				s.state = sendError
				continue outer
			}

			s.request = request
			s.state = sendRequest
		case sendRequest:
			err := s.sendTransferRequest(stream)
			if err != nil {
				sendErr = err
				s.state = sendError
				continue outer
			}

			s.state = sendWaitingResponse
		case sendWaitingResponse:
			err := s.recieveTransferResponse(stream)
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
			err := s.sendData(reader, stream)
			if err != nil {
				sendErr = err
				s.state = sendError
				continue outer
			}

			s.state = sendDone
		case sendDone:
			break outer
		case sendRejected:
			break outer
		case sendError:
			break outer
		}
	}

	return &sendResult{
		Accepted:     accepted,
		RejectReason: rejectReason,
		Err:          sendErr,
	}
}

func initRequest(reader file.Reader, senderAddr net.Addr) (*models.TransferRequest, error) {
	stat, err := reader.Stat()
	if err != nil {
		return nil, err
	}

	mimeType, err := detectMIME(reader)
	if err != nil {
		return nil, err
	}

	host, _, err := net.SplitHostPort(senderAddr.String())
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

func detectMIME(reader file.Reader) (string, error) {
	buf := make([]byte, mimeDetectionBufferSizeBytes)

	n, err := io.ReadFull(reader, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return "", err
	}

	_, seekErr := reader.Seek(0, io.SeekStart)
	if seekErr != nil {
		return "", seekErr
	}

	if n == 0 {
		return "application/octet-stream", nil
	}

	return http.DetectContentType(buf[:n]), nil
}

func (s *transferSend) sendTransferRequest(stream Stream) error {
	if s == nil {
		return fmt.Errorf("<s> transferSend is NULL")
	}

	if s.request == nil {
		return fmt.Errorf("request is NULL when sending transfer request")
	}

	err := writeMessage(stream, s.request)
	if err != nil {
		return err
	}

	return nil
}

func (s *transferSend) recieveTransferResponse(stream Stream) error {
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

	err := readMessage(stream, s.response)
	if err != nil {
		return err
	}

	return nil
}

func (s *transferSend) sendData(reader file.Reader, stream Stream) error {
	buf := make([]byte, fileReadChunkSizeBytes)
	for {
		bytesRead, err := reader.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		if bytesRead > 0 {
			_, err := stream.Write(buf[:bytesRead])

			if err != nil {
				return err
			}
		}
	}

	return nil
}
