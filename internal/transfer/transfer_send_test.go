package transfer

import (
	"airlink/internal/transfer/models"
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type transferTestFileInfo struct {
	name    string
	size    int64
	modTime time.Time
}

func (i *transferTestFileInfo) Name() string {
	return i.name
}

func (i *transferTestFileInfo) Size() int64 {
	return i.size
}

func (i *transferTestFileInfo) Mode() os.FileMode {
	return 0o600
}

func (i *transferTestFileInfo) ModTime() time.Time {
	return i.modTime
}

func (i *transferTestFileInfo) IsDir() bool {
	return false
}

func (i *transferTestFileInfo) Sys() any {
	return nil
}

type mockTransferReader struct {
	data     []byte
	pos      int
	stat     os.FileInfo
	statErr  error
	readErr  error
	seekErr  error
	closeErr error
}

func newMockTransferReader(name string, data []byte) *mockTransferReader {
	return &mockTransferReader{
		data: append([]byte(nil), data...),
		stat: &transferTestFileInfo{
			name:    name,
			size:    int64(len(data)),
			modTime: time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC),
		},
	}
}

func (r *mockTransferReader) Read(p []byte) (int, error) {
	if r.readErr != nil {
		return 0, r.readErr
	}

	if r.pos >= len(r.data) {
		return 0, io.EOF
	}

	n := copy(p, r.data[r.pos:])
	r.pos += n

	return n, nil
}

func (r *mockTransferReader) Stat() (os.FileInfo, error) {
	if r.statErr != nil {
		return nil, r.statErr
	}

	return r.stat, nil
}

func (r *mockTransferReader) Seek(offset int64, whence int) (int64, error) {
	if r.seekErr != nil {
		return 0, r.seekErr
	}

	var target int64

	switch whence {
	case io.SeekStart:
		target = offset
	case io.SeekCurrent:
		target = int64(r.pos) + offset
	case io.SeekEnd:
		target = int64(len(r.data)) + offset
	default:
		return 0, errors.New("unsupported seek mode")
	}

	if target < 0 {
		return 0, errors.New("negative seek position")
	}

	r.pos = int(target)

	return target, nil
}

func (r *mockTransferReader) Close() error {
	return r.closeErr
}

func TestDetectMIME(t *testing.T) {
	t.Parallel()

	t.Run("detects PNG and rewinds reader", func(t *testing.T) {
		t.Parallel()

		data := []byte{
			0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A,
		}
		reader := newMockTransferReader("picture.png", data)

		mimeType, err := detectMIME(reader)

		require.NoError(t, err)
		assert.Equal(t, "image/png", mimeType)
		assert.Zero(t, reader.pos)
	})

	t.Run("handles files smaller than 512 bytes", func(t *testing.T) {
		t.Parallel()

		reader := newMockTransferReader("hello.txt", []byte("hello"))

		mimeType, err := detectMIME(reader)

		require.NoError(t, err)
		assert.Equal(t, "text/plain; charset=utf-8", mimeType)
		assert.Zero(t, reader.pos)
	})

	t.Run("handles empty file", func(t *testing.T) {
		t.Parallel()

		reader := newMockTransferReader("empty", nil)

		mimeType, err := detectMIME(reader)

		require.NoError(t, err)
		assert.Equal(t, "application/octet-stream", mimeType)
		assert.Zero(t, reader.pos)
	})

	t.Run("read error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("read failed")
		reader := newMockTransferReader("picture.png", []byte("data"))
		reader.readErr = expectedErr

		_, err := detectMIME(reader)

		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("seek error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("seek failed")
		reader := newMockTransferReader("hello.txt", []byte("hello"))
		reader.seekErr = expectedErr

		_, err := detectMIME(reader)

		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
	})
}

func TestInitRequest(t *testing.T) {
	t.Parallel()

	senderAddr := &net.UDPAddr{
		IP:   net.ParseIP("192.0.2.10"),
		Port: 12345,
	}

	t.Run("initializes request and rewinds reader", func(t *testing.T) {
		t.Parallel()

		data := []byte{
			0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A,
		}
		reader := newMockTransferReader("picture.png", data)

		request, err := initRequest(reader, senderAddr)

		require.NoError(t, err)
		require.NotNil(t, request)

		assert.Equal(t, "picture.png", request.GetFilename())
		assert.Equal(t, ".png", request.GetExtension())
		assert.Equal(t, uint64(len(data)), request.GetFileSize())
		assert.Equal(t, "image/png", request.GetMime())
		assert.Equal(t, "192.0.2.10", request.GetSenderIp())

		assert.Equal(t, senderName, request.GetSenderName())
		assert.Equal(t, clientName, request.GetClientName())
		assert.Equal(t, clientVersion, request.GetClientVersion())

		require.NotNil(t, request.GetModifiedAt())
		assert.Equal(
			t,
			reader.stat.ModTime(),
			request.GetModifiedAt().AsTime(),
		)

		assert.Zero(t, reader.pos)
	})

	t.Run("stat error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stat failed")
		reader := newMockTransferReader("picture.png", []byte("data"))
		reader.statErr = expectedErr

		request, err := initRequest(reader, senderAddr)

		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Nil(t, request)
	})

	t.Run("MIME detection error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("read failed")
		reader := newMockTransferReader("picture.png", []byte("data"))
		reader.readErr = expectedErr

		request, err := initRequest(reader, senderAddr)

		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Nil(t, request)
	})

	t.Run("invalid sender address", func(t *testing.T) {
		t.Parallel()

		reader := newMockTransferReader("picture.png", []byte("data"))
		invalidAddr := &net.IPAddr{
			IP: net.ParseIP("192.0.2.10"),
		}

		request, err := initRequest(reader, invalidAddr)

		require.Error(t, err)
		assert.Nil(t, request)
		assert.Zero(t, reader.pos)
	})
}

func TestSendTransferRequest(t *testing.T) {
	t.Parallel()

	t.Run("transfer is nil", func(t *testing.T) {
		t.Parallel()

		var sender *transferSend

		err := sender.sendTransferRequest(&mockStream{})

		require.Error(t, err)
	})

	t.Run("request is nil", func(t *testing.T) {
		t.Parallel()

		sender := &transferSend{}

		err := sender.sendTransferRequest(&mockStream{})

		require.Error(t, err)
	})

	t.Run("stream write fails", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream write failed")

		sender := &transferSend{
			request: &models.TransferRequest{
				Filename: "hello.txt",
			},
		}

		stream := &mockStream{
			writeFunc: func([]byte) (int, error) {
				return 0, expectedErr
			},
		}

		err := sender.sendTransferRequest(stream)

		require.ErrorIs(t, err, expectedErr)
	})

	t.Run("writes request successfully", func(t *testing.T) {
		t.Parallel()

		sender := &transferSend{
			request: &models.TransferRequest{
				Filename: "hello.txt",
				FileSize: 5,
			},
		}

		var written bytes.Buffer
		stream := &mockStream{
			writeFunc: written.Write,
		}

		err := sender.sendTransferRequest(stream)

		require.NoError(t, err)

		received := &models.TransferRequest{}
		err = readMessage(bytes.NewReader(written.Bytes()), received)

		require.NoError(t, err)
		assert.Equal(t, sender.request.GetFilename(), received.GetFilename())
		assert.Equal(t, sender.request.GetFileSize(), received.GetFileSize())
	})
}

func TestReceiveTransferResponse(t *testing.T) {
	t.Parallel()

	t.Run("rejects an invalid state", func(t *testing.T) {
		t.Parallel()

		sender := &transferSend{
			state:    sendInit,
			response: &models.TransferResponse{},
		}

		err := sender.recieveTransferResponse(&mockStream{})

		require.Error(t, err)
	})

	t.Run("rejects an uninitialized response", func(t *testing.T) {
		t.Parallel()

		sender := &transferSend{
			state: sendWaitingResponse,
		}

		err := sender.recieveTransferResponse(&mockStream{})

		require.Error(t, err)
	})

	t.Run("reads and populates accepted response", func(t *testing.T) {
		t.Parallel()

		var responseBuffer bytes.Buffer
		require.NoError(t, writeMessage(
			&responseBuffer,
			&models.TransferResponse{
				Accepted: true,
			},
		))

		responseReader := bytes.NewReader(responseBuffer.Bytes())

		sender := &transferSend{
			state:    sendWaitingResponse,
			response: &models.TransferResponse{},
		}

		stream := &mockStream{
			readFunc: responseReader.Read,
		}

		err := sender.recieveTransferResponse(stream)

		require.NoError(t, err)
		assert.True(t, sender.response.GetAccepted())
	})

	t.Run("reads and populates rejection reason", func(t *testing.T) {
		t.Parallel()

		reason := "destination already exists"

		var responseBuffer bytes.Buffer
		require.NoError(t, writeMessage(
			&responseBuffer,
			&models.TransferResponse{
				Accepted:     false,
				RejectReason: &reason,
			},
		))

		responseReader := bytes.NewReader(responseBuffer.Bytes())

		sender := &transferSend{
			state:    sendWaitingResponse,
			response: &models.TransferResponse{},
		}

		stream := &mockStream{
			readFunc: responseReader.Read,
		}

		err := sender.recieveTransferResponse(stream)

		require.NoError(t, err)
		assert.False(t, sender.response.GetAccepted())
		require.NotNil(t, sender.response.RejectReason)
		assert.Equal(t, reason, sender.response.GetRejectReason())
	})

	t.Run("propagates stream read error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream read failed")

		sender := &transferSend{
			state:    sendWaitingResponse,
			response: &models.TransferResponse{},
		}

		stream := &mockStream{
			readFunc: func([]byte) (int, error) {
				return 0, expectedErr
			},
		}

		err := sender.recieveTransferResponse(stream)

		require.ErrorIs(t, err, expectedErr)
	})
}

func TestTransferSendRun(t *testing.T) {
	t.Parallel()

	senderAddr := &net.UDPAddr{
		IP:   net.ParseIP("192.0.2.10"),
		Port: 12345,
	}

	t.Run("sends request and accepts response", func(t *testing.T) {
		t.Parallel()

		reader := newMockTransferReader("hello.txt", []byte("hello"))

		var requestBuffer bytes.Buffer
		response := &models.TransferResponse{
			Accepted: true,
		}
		var responseBuffer bytes.Buffer
		require.NoError(t, writeMessage(&responseBuffer, response))
		responseReader := bytes.NewReader(responseBuffer.Bytes())

		stream := &mockStream{
			writeFunc: requestBuffer.Write,
			readFunc:  responseReader.Read,
		}

		sender := &transferSend{
			state:    sendInit,
			response: &models.TransferResponse{},
		}

		result := sender.run(reader, stream, senderAddr)

		require.NotNil(t, result)
		require.NoError(t, result.Err)
		assert.True(t, result.Accepted)
		assert.Nil(t, result.RejectReason)
		assert.Equal(t, sendSending, sender.state)

		require.NotNil(t, sender.request)
		assert.Equal(t, "hello.txt", sender.request.GetFilename())
		assert.Zero(t, reader.pos)
		assert.NotZero(t, requestBuffer.Len())

		received := &models.TransferRequest{}
		err := readMessage(bytes.NewReader(requestBuffer.Bytes()), received)

		require.NoError(t, err)
		assert.Equal(t, sender.request.GetFilename(), received.GetFilename())
		assert.Equal(t, sender.request.GetFileSize(), received.GetFileSize())
		assert.Equal(t, sender.request.GetMime(), received.GetMime())
		assert.Equal(t, sender.request.GetSenderIp(), received.GetSenderIp())
	})

	t.Run("returns rejection reason", func(t *testing.T) {
		t.Parallel()

		reason := "destination already exists"

		var responseBuffer bytes.Buffer
		require.NoError(t, writeMessage(
			&responseBuffer,
			&models.TransferResponse{
				Accepted:     false,
				RejectReason: &reason,
			},
		))

		responseReader := bytes.NewReader(responseBuffer.Bytes())

		sender := &transferSend{
			state:    sendWaitingResponse,
			response: &models.TransferResponse{},
		}

		stream := &mockStream{
			readFunc: responseReader.Read,
		}

		result := sender.run(
			newMockTransferReader("hello.txt", []byte("hello")),
			stream,
			senderAddr,
		)

		require.NotNil(t, result)
		require.NoError(t, result.Err)
		assert.False(t, result.Accepted)
		assert.Equal(t, sendRejected, sender.state)
		require.NotNil(t, result.RejectReason)
		assert.Equal(t, reason, *result.RejectReason)
	})

	t.Run("stops when request initialization fails", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stat failed")
		reader := newMockTransferReader("hello.txt", []byte("hello"))
		reader.statErr = expectedErr

		writeCalls := 0
		stream := &mockStream{
			writeFunc: func([]byte) (int, error) {
				writeCalls++
				return 0, nil
			},
		}

		sender := &transferSend{
			state: sendInit,
		}

		result := sender.run(reader, stream, senderAddr)

		require.NotNil(t, result)
		require.ErrorIs(t, result.Err, expectedErr)
		assert.Equal(t, sendError, sender.state)
		assert.Nil(t, sender.request)
		assert.Zero(t, writeCalls)
	})

	t.Run("stops when sending request fails", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream write failed")
		reader := newMockTransferReader("hello.txt", []byte("hello"))

		stream := &mockStream{
			writeFunc: func([]byte) (int, error) {
				return 0, expectedErr
			},
		}

		sender := &transferSend{
			state: sendInit,
		}

		result := sender.run(reader, stream, senderAddr)

		require.NotNil(t, result)
		require.ErrorIs(t, result.Err, expectedErr)
		assert.Equal(t, sendError, sender.state)
		assert.NotNil(t, sender.request)
	})

	t.Run("stops when reading response fails", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream read failed")

		sender := &transferSend{
			state:    sendWaitingResponse,
			response: &models.TransferResponse{},
		}

		stream := &mockStream{
			readFunc: func([]byte) (int, error) {
				return 0, expectedErr
			},
		}

		result := sender.run(
			newMockTransferReader("hello.txt", []byte("hello")),
			stream,
			senderAddr,
		)

		require.NotNil(t, result)
		require.ErrorIs(t, result.Err, expectedErr)
		assert.Equal(t, sendError, sender.state)
		assert.False(t, result.Accepted)
	})
}
