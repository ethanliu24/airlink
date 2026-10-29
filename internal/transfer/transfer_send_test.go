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

// Fails on reads after MIME detection has rewound the reader.
// This lets the full state machine reach sendSending before failing.
type failAfterRewindReader struct {
	*mockTransferReader
	err      error
	rewound  bool
}

func (r *failAfterRewindReader) Read(p []byte) (int, error) {
	if r.rewound {
		return 0, r.err
	}

	return r.mockTransferReader.Read(p)
}

func (r *failAfterRewindReader) Seek(offset int64, whence int) (int64, error) {
	n, err := r.mockTransferReader.Seek(offset, whence)
	if err == nil && offset == 0 && whence == io.SeekStart {
		r.rewound = true
	}

	return n, err
}

type transferTestStream struct {
	readFrom    io.Reader
	readErr     error
	writeErr    error
	failWriteAt int
	writeCalls  int
	written     bytes.Buffer
}

func (s *transferTestStream) Read(p []byte) (int, error) {
	if s.readErr != nil {
		return 0, s.readErr
	}

	if s.readFrom != nil {
		return s.readFrom.Read(p)
	}

	return 0, io.EOF
}

func (s *transferTestStream) Write(p []byte) (int, error) {
	s.writeCalls++

	if s.failWriteAt > 0 && s.writeCalls >= s.failWriteAt {
		if s.writeErr != nil {
			return 0, s.writeErr
		}

		return 0, errors.New("mock stream write failed")
	}

	if s.failWriteAt == 0 && s.writeErr != nil {
		return 0, s.writeErr
	}

	return s.written.Write(p)
}

func (s *transferTestStream) Close() error {
	return nil
}

func newTransferResponseReader(
	t *testing.T,
	response *models.TransferResponse,
) *bytes.Reader {
	t.Helper()

	var buf bytes.Buffer
	require.NoError(t, writeMessage(&buf, response))

	return bytes.NewReader(buf.Bytes())
}

func newTestSender(reader *mockTransferReader) *transferSend {
	return NewTransferSend(
		reader,
		&transferTestStream{},
		&net.UDPAddr{
			IP:   net.ParseIP("192.0.2.10"),
			Port: 12345,
		},
	)
}

func TestDetectMIME(t *testing.T) {
	t.Parallel()

	t.Run("detects PNG and rewinds reader", func(t *testing.T) {
		t.Parallel()

		data := []byte{
			0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A,
		}
		reader := newMockTransferReader("picture.png", data)
		sender := newTestSender(reader)

		mimeType, err := sender.detectMIME()

		require.NoError(t, err)
		assert.Equal(t, "image/png", mimeType)
		assert.Zero(t, reader.pos)
	})

	t.Run("handles files smaller than 512 bytes", func(t *testing.T) {
		t.Parallel()

		reader := newMockTransferReader("hello.txt", []byte("hello"))
		sender := newTestSender(reader)

		mimeType, err := sender.detectMIME()

		require.NoError(t, err)
		assert.Equal(t, "text/plain; charset=utf-8", mimeType)
		assert.Zero(t, reader.pos)
	})

	t.Run("handles empty file", func(t *testing.T) {
		t.Parallel()

		reader := newMockTransferReader("empty", nil)
		sender := newTestSender(reader)

		mimeType, err := sender.detectMIME()

		require.NoError(t, err)
		assert.Equal(t, "application/octet-stream", mimeType)
		assert.Zero(t, reader.pos)
	})

	t.Run("propagates read error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("read failed")
		reader := newMockTransferReader("picture.png", []byte("data"))
		reader.readErr = expectedErr
		sender := newTestSender(reader)

		_, err := sender.detectMIME()

		require.ErrorIs(t, err, expectedErr)
	})

	t.Run("propagates seek error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("seek failed")
		reader := newMockTransferReader("hello.txt", []byte("hello"))
		reader.seekErr = expectedErr
		sender := newTestSender(reader)

		_, err := sender.detectMIME()

		require.ErrorIs(t, err, expectedErr)
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
		sender := NewTransferSend(reader, &transferTestStream{}, senderAddr)

		request, err := sender.initRequest()

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

	t.Run("rejects invalid state", func(t *testing.T) {
		t.Parallel()

		sender := newTestSender(newMockTransferReader("report.txt", []byte("data")))
		sender.state = sendRequest

		request, err := sender.initRequest()

		require.Error(t, err)
		assert.Nil(t, request)
	})

	t.Run("propagates stat error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stat failed")
		reader := newMockTransferReader("picture.png", []byte("data"))
		reader.statErr = expectedErr
		sender := NewTransferSend(reader, &transferTestStream{}, senderAddr)

		request, err := sender.initRequest()

		require.ErrorIs(t, err, expectedErr)
		assert.Nil(t, request)
	})

	t.Run("propagates MIME detection error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("read failed")
		reader := newMockTransferReader("picture.png", []byte("data"))
		reader.readErr = expectedErr
		sender := NewTransferSend(reader, &transferTestStream{}, senderAddr)

		request, err := sender.initRequest()

		require.ErrorIs(t, err, expectedErr)
		assert.Nil(t, request)
	})

	t.Run("rejects invalid sender address", func(t *testing.T) {
		t.Parallel()

		reader := newMockTransferReader("picture.png", []byte("data"))
		invalidAddr := &net.IPAddr{
			IP: net.ParseIP("192.0.2.10"),
		}
		sender := NewTransferSend(reader, &transferTestStream{}, invalidAddr)

		request, err := sender.initRequest()

		require.Error(t, err)
		assert.Nil(t, request)
		assert.Zero(t, reader.pos)
	})
}

func TestSendTransferRequest(t *testing.T) {
	t.Parallel()

	t.Run("rejects invalid state", func(t *testing.T) {
		t.Parallel()

		sender := newTestSender(newMockTransferReader("hello.txt", []byte("hello")))

		err := sender.sendTransferRequest()

		require.Error(t, err)
	})

	t.Run("propagates stream write error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream write failed")
		stream := &transferTestStream{
			writeErr: expectedErr,
		}
		sender := NewTransferSend(
			newMockTransferReader("hello.txt", []byte("hello")),
			stream,
			&net.UDPAddr{IP: net.ParseIP("192.0.2.10"), Port: 12345},
		)
		sender.state = sendRequest
		sender.request = &models.TransferRequest{
			Filename: "hello.txt",
			FileSize: 5,
		}

		err := sender.sendTransferRequest()

		require.ErrorIs(t, err, expectedErr)
	})

	t.Run("writes request successfully", func(t *testing.T) {
		t.Parallel()

		stream := &transferTestStream{}
		sender := NewTransferSend(
			newMockTransferReader("hello.txt", []byte("hello")),
			stream,
			&net.UDPAddr{IP: net.ParseIP("192.0.2.10"), Port: 12345},
		)
		sender.state = sendRequest
		sender.request = &models.TransferRequest{
			Filename: "hello.txt",
			FileSize: 5,
		}

		err := sender.sendTransferRequest()

		require.NoError(t, err)

		received := &models.TransferRequest{}
		err = readMessage(bytes.NewReader(stream.written.Bytes()), received)

		require.NoError(t, err)
		assert.Equal(t, sender.request.GetFilename(), received.GetFilename())
		assert.Equal(t, sender.request.GetFileSize(), received.GetFileSize())
	})
}

func TestRecieveTransferResponse(t *testing.T) {
	t.Parallel()

	t.Run("rejects invalid state", func(t *testing.T) {
		t.Parallel()

		sender := newTestSender(newMockTransferReader("hello.txt", []byte("hello")))
		sender.response = &models.TransferResponse{}

		err := sender.recieveTransferResponse()

		require.Error(t, err)
	})

	t.Run("rejects uninitialized response", func(t *testing.T) {
		t.Parallel()

		sender := newTestSender(newMockTransferReader("hello.txt", []byte("hello")))
		sender.state = sendWaitingResponse
		sender.response = nil

		err := sender.recieveTransferResponse()

		require.Error(t, err)
	})

	t.Run("reads accepted response", func(t *testing.T) {
		t.Parallel()

		responseReader := newTransferResponseReader(
			t,
			&models.TransferResponse{Accepted: true},
		)
		stream := &transferTestStream{readFrom: responseReader}
		sender := NewTransferSend(
			newMockTransferReader("hello.txt", []byte("hello")),
			stream,
			&net.UDPAddr{IP: net.ParseIP("192.0.2.10"), Port: 12345},
		)
		sender.state = sendWaitingResponse

		err := sender.recieveTransferResponse()

		require.NoError(t, err)
		assert.True(t, sender.response.GetAccepted())
	})

	t.Run("reads rejection reason", func(t *testing.T) {
		t.Parallel()

		reason := "destination already exists"
		responseReader := newTransferResponseReader(
			t,
			&models.TransferResponse{
				Accepted:     false,
				RejectReason: &reason,
			},
		)
		stream := &transferTestStream{readFrom: responseReader}
		sender := NewTransferSend(
			newMockTransferReader("hello.txt", []byte("hello")),
			stream,
			&net.UDPAddr{IP: net.ParseIP("192.0.2.10"), Port: 12345},
		)
		sender.state = sendWaitingResponse

		err := sender.recieveTransferResponse()

		require.NoError(t, err)
		assert.False(t, sender.response.GetAccepted())
		require.NotNil(t, sender.response.RejectReason)
		assert.Equal(t, reason, sender.response.GetRejectReason())
	})

	t.Run("propagates stream read error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream read failed")
		stream := &transferTestStream{readErr: expectedErr}
		sender := NewTransferSend(
			newMockTransferReader("hello.txt", []byte("hello")),
			stream,
			&net.UDPAddr{IP: net.ParseIP("192.0.2.10"), Port: 12345},
		)
		sender.state = sendWaitingResponse

		err := sender.recieveTransferResponse()

		require.ErrorIs(t, err, expectedErr)
	})
}

func TestSendData(t *testing.T) {
	t.Parallel()

	t.Run("sends complete file contents", func(t *testing.T) {
		t.Parallel()

		data := bytes.Repeat(
			[]byte{0x00, 0x01, 0xFE, 0xFF},
			fileReadChunkSizeBytes/4+10,
		)
		reader := newMockTransferReader("binary.dat", data)
		stream := &transferTestStream{}
		sender := NewTransferSend(
			reader,
			stream,
			&net.UDPAddr{IP: net.ParseIP("192.0.2.10"), Port: 12345},
		)
		sender.state = sendSending

		err := sender.sendData()

		require.NoError(t, err)
		assert.Equal(t, data, stream.written.Bytes())
		assert.Greater(t, stream.writeCalls, 1)
		assert.Equal(t, len(data), reader.pos)
	})

	t.Run("empty file sends no data", func(t *testing.T) {
		t.Parallel()

		reader := newMockTransferReader("empty", nil)
		stream := &transferTestStream{}
		sender := NewTransferSend(
			reader,
			stream,
			&net.UDPAddr{IP: net.ParseIP("192.0.2.10"), Port: 12345},
		)
		sender.state = sendSending

		err := sender.sendData()

		require.NoError(t, err)
		assert.Zero(t, stream.writeCalls)
		assert.Empty(t, stream.written.Bytes())
	})

	t.Run("rejects invalid state", func(t *testing.T) {
		t.Parallel()

		sender := newTestSender(newMockTransferReader("hello.txt", []byte("hello")))

		err := sender.sendData()

		require.Error(t, err)
	})

	t.Run("propagates reader error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("file read failed")
		reader := newMockTransferReader("hello.txt", []byte("hello"))
		reader.readErr = expectedErr
		sender := NewTransferSend(
			reader,
			&transferTestStream{},
			&net.UDPAddr{IP: net.ParseIP("192.0.2.10"), Port: 12345},
		)
		sender.state = sendSending

		err := sender.sendData()

		require.ErrorIs(t, err, expectedErr)
	})

	t.Run("propagates stream write error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream write failed")
		reader := newMockTransferReader("hello.txt", []byte("hello"))
		stream := &transferTestStream{writeErr: expectedErr}
		sender := NewTransferSend(
			reader,
			stream,
			&net.UDPAddr{IP: net.ParseIP("192.0.2.10"), Port: 12345},
		)
		sender.state = sendSending

		err := sender.sendData()

		require.ErrorIs(t, err, expectedErr)
	})
}

func TestTransferSendRun(t *testing.T) {
	t.Parallel()

	senderAddr := &net.UDPAddr{
		IP:   net.ParseIP("192.0.2.10"),
		Port: 12345,
	}

	t.Run("sends request and file when accepted", func(t *testing.T) {
		t.Parallel()

		fileData := []byte("hello file contents")
		reader := newMockTransferReader("hello.txt", fileData)
		responseReader := newTransferResponseReader(
			t,
			&models.TransferResponse{Accepted: true},
		)
		stream := &transferTestStream{readFrom: responseReader}
		sender := NewTransferSend(reader, stream, senderAddr)

		result := sender.run()

		require.NotNil(t, result)
		require.NoError(t, result.Err)
		assert.True(t, result.Accepted)
		assert.Nil(t, result.RejectReason)
		assert.Equal(t, sendDone, sender.state)
		assert.Equal(t, len(fileData), reader.pos)

		// The output consists of a framed protobuf request followed by raw file bytes.
		out := bytes.NewReader(stream.written.Bytes())
		received := &models.TransferRequest{}

		require.NoError(t, readMessage(out, received))
		assert.Equal(t, sender.request.GetFilename(), received.GetFilename())
		assert.Equal(t, sender.request.GetFileSize(), received.GetFileSize())
		assert.Equal(t, sender.request.GetMime(), received.GetMime())

		transferredData, err := io.ReadAll(out)
		require.NoError(t, err)
		assert.Equal(t, fileData, transferredData)
	})

	t.Run("returns rejection reason without sending file", func(t *testing.T) {
		t.Parallel()

		reason := "destination already exists"
		reader := newMockTransferReader("hello.txt", []byte("hello"))
		responseReader := newTransferResponseReader(
			t,
			&models.TransferResponse{
				Accepted:     false,
				RejectReason: &reason,
			},
		)
		stream := &transferTestStream{readFrom: responseReader}
		sender := NewTransferSend(reader, stream, senderAddr)

		result := sender.run()

		require.NotNil(t, result)
		require.NoError(t, result.Err)
		assert.False(t, result.Accepted)
		assert.Equal(t, sendRejected, sender.state)
		require.NotNil(t, result.RejectReason)
		assert.Equal(t, reason, *result.RejectReason)

		out := bytes.NewReader(stream.written.Bytes())
		received := &models.TransferRequest{}

		require.NoError(t, readMessage(out, received))
		assert.Equal(t, "hello.txt", received.GetFilename())
		assert.Zero(t, out.Len())
		assert.Zero(t, reader.pos)
	})

	t.Run("stops when request initialization fails", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stat failed")
		reader := newMockTransferReader("hello.txt", []byte("hello"))
		reader.statErr = expectedErr
		stream := &transferTestStream{}
		sender := NewTransferSend(reader, stream, senderAddr)

		result := sender.run()

		require.NotNil(t, result)
		require.ErrorIs(t, result.Err, expectedErr)
		assert.Equal(t, sendError, sender.state)
		assert.Nil(t, sender.request)
		assert.Zero(t, stream.writeCalls)
	})

	t.Run("stops when sending request fails", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream write failed")
		reader := newMockTransferReader("hello.txt", []byte("hello"))
		stream := &transferTestStream{writeErr: expectedErr}
		sender := NewTransferSend(reader, stream, senderAddr)

		result := sender.run()

		require.NotNil(t, result)
		require.ErrorIs(t, result.Err, expectedErr)
		assert.Equal(t, sendError, sender.state)
		assert.NotNil(t, sender.request)
	})

	t.Run("stops when receiving response fails", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream read failed")
		reader := newMockTransferReader("hello.txt", []byte("hello"))
		stream := &transferTestStream{readErr: expectedErr}
		sender := NewTransferSend(reader, stream, senderAddr)

		result := sender.run()

		require.NotNil(t, result)
		require.ErrorIs(t, result.Err, expectedErr)
		assert.Equal(t, sendError, sender.state)
	})

	t.Run("stops when reading file data fails", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("file read failed")
		reader := &failAfterRewindReader{
			mockTransferReader: newMockTransferReader("hello.txt", []byte("hello")),
			err:                expectedErr,
		}
		responseReader := newTransferResponseReader(
			t,
			&models.TransferResponse{Accepted: true},
		)
		stream := &transferTestStream{readFrom: responseReader}
		sender := NewTransferSend(reader, stream, senderAddr)

		result := sender.run()

		require.NotNil(t, result)
		require.ErrorIs(t, result.Err, expectedErr)
		assert.True(t, result.Accepted)
		assert.Equal(t, sendError, sender.state)
	})

	t.Run("stops when writing file data fails", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("file write failed")
		reader := newMockTransferReader("hello.txt", []byte("hello"))
		responseReader := newTransferResponseReader(
			t,
			&models.TransferResponse{Accepted: true},
		)

		// writeMessage writes the request header and protobuf first.
		// The third Write call is the first file-data write.
		stream := &transferTestStream{
			readFrom:    responseReader,
			writeErr:    expectedErr,
			failWriteAt: 3,
		}
		sender := NewTransferSend(reader, stream, senderAddr)

		result := sender.run()

		require.NotNil(t, result)
		require.ErrorIs(t, result.Err, expectedErr)
		assert.True(t, result.Accepted)
		assert.Equal(t, sendError, sender.state)
	})
}
