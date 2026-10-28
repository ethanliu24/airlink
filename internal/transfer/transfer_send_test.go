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

func (i transferTestFileInfo) Name() string {
	return i.name
}

func (i transferTestFileInfo) Size() int64 {
	return i.size
}

func (i transferTestFileInfo) Mode() os.FileMode {
	return 0o600
}

func (i transferTestFileInfo) ModTime() time.Time {
	return i.modTime
}

func (i transferTestFileInfo) IsDir() bool {
	return false
}

func (i transferTestFileInfo) Sys() any {
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
		stat: transferTestFileInfo{
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

		err := sendTransferRequest(nil, &mockStream{})

		require.Error(t, err)
	})

	t.Run("request is nil", func(t *testing.T) {
		t.Parallel()

		transfer := &transferSend{}

		err := sendTransferRequest(transfer, &mockStream{})

		require.Error(t, err)
	})

	t.Run("stream write fails", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream write failed")

		transfer := &transferSend{
			request: &models.TransferRequest{
				Filename: "hello.txt",
			},
		}

		stream := &mockStream{
			writeFunc: func([]byte) (int, error) {
				return 0, expectedErr
			},
		}

		err := sendTransferRequest(transfer, stream)

		require.ErrorIs(t, err, expectedErr)
	})
}

func TestTransferSendRun(t *testing.T) {
	t.Parallel()

	senderAddr := &net.UDPAddr{
		IP:   net.ParseIP("192.0.2.10"),
		Port: 12345,
	}

	t.Run("sends request and advances to waiting response", func(t *testing.T) {
		t.Parallel()

		reader := newMockTransferReader("hello.txt", []byte("hello"))

		var written bytes.Buffer
		stream := &mockStream{
			writeFunc: written.Write,
		}

		sender := &transferSend{
			state: sendInit,
		}

		err := sender.run(reader, stream, senderAddr)

		require.NoError(t, err)
		assert.Equal(t, sendWaitingResponse, sender.state)
		assert.NoError(t, sender.err)

		require.NotNil(t, sender.request)
		assert.Equal(t, "hello.txt", sender.request.GetFilename())
		assert.Zero(t, reader.pos)
		assert.NotZero(t, written.Len())

		// Verify the serialized request written to the stream.
		received := &models.TransferRequest{}
		err = readMessage(bytes.NewReader(written.Bytes()), received)

		require.NoError(t, err)
		assert.Equal(t, sender.request.GetFilename(), received.GetFilename())
		assert.Equal(t, sender.request.GetFileSize(), received.GetFileSize())
		assert.Equal(t, sender.request.GetMime(), received.GetMime())
		assert.Equal(t, sender.request.GetSenderIp(), received.GetSenderIp())
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

		err := sender.run(reader, stream, senderAddr)

		require.ErrorIs(t, err, expectedErr)
		assert.Equal(t, sendError, sender.state)
		assert.ErrorIs(t, sender.err, expectedErr)
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

		err := sender.run(reader, stream, senderAddr)

		require.ErrorIs(t, err, expectedErr)
		assert.Equal(t, sendError, sender.state)
		assert.ErrorIs(t, sender.err, expectedErr)
		assert.NotNil(t, sender.request)
	})
}
