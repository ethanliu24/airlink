package transfer

import (
	"airlink/internal/file"
	"airlink/internal/transfer/models"
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
	MIME_DETECTION_BUFFER_SIZE_BYTES = 512
)

type TransferSend struct {
	state   sendState
	request *models.TransferRequest
	err     error
}

// Assume caller will handle opening and closing file
// This function should only be called once per TransferSend
func (s *TransferSend) run(reader file.Reader, senderAddr net.Addr) error {
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

func detectMIME(reader file.Reader) (string, error) {
	buf := make([]byte, MIME_DETECTION_BUFFER_SIZE_BYTES)

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
