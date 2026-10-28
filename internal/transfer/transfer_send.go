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
	sendSending         sendState = "sending"
	sendSuccess         sendState = "success"
	sendError           sendState = "error"
)

const (
	mimeDetectionBufferSizeBytes = 512
)

type transferSend struct {
	state   sendState
	request *models.TransferRequest
	err     error
}

// Assume caller will handle opening and closing the file and stream
// This should run synchronously
// This function should only be called once per TransferSend
func (s *transferSend) run(reader file.Reader, stream Stream, senderAddr net.Addr) error {
	// TODO make sure this function is called once max per TransferSend
outer:
	for {
		switch s.state {
		case sendInit:
			request, err := initRequest(reader, senderAddr)
			if err != nil {
				s.err = err
				s.state = sendError
				continue outer
			}

			s.request = request
			s.state = sendRequest
		case sendRequest:
			err := sendTransferRequest(s, stream)
			if err != nil {
				s.err = err
				s.state = sendError
				continue outer
			}

			s.state = sendWaitingResponse
		case sendWaitingResponse:
			break outer
		case sendError:
			break outer
		}
	}

	return s.err
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

func sendTransferRequest(s *transferSend, stream Stream) error {
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
