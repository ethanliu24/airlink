
package transfer

import (
	"airlink/internal/file"
	"airlink/internal/transfer/models"
	
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testAcceptWaitTimeout = 100 * time.Millisecond

var errSendTestStreamAborted = errors.New("send test stream aborted")

type sendTestFileInfo struct {
	name    string
	size    int64
	modTime time.Time
}

func (i *sendTestFileInfo) Name() string {
	return i.name
}

func (i *sendTestFileInfo) Size() int64 {
	return i.size
}

func (i *sendTestFileInfo) Mode() os.FileMode {
	return 0o600
}

func (i *sendTestFileInfo) ModTime() time.Time {
	return i.modTime
}

func (i *sendTestFileInfo) IsDir() bool {
	return false
}

func (i *sendTestFileInfo) Sys() any {
	return nil
}

type sendTestReader struct {
	data       []byte
	pos        int
	stat       os.FileInfo
	statErr    error
	readErr    error
	seekErr    error
	closeErr   error
	closeCalls int
}

func newSendTestReader(name string, data []byte) *sendTestReader {
	return &sendTestReader{
		data: append([]byte(nil), data...),
		stat: &sendTestFileInfo{
			name:    name,
			size:    int64(len(data)),
			modTime: time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC),
		},
	}
}

func (r *sendTestReader) Read(p []byte) (int, error) {
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

func (r *sendTestReader) Stat() (os.FileInfo, error) {
	if r.statErr != nil {
		return nil, r.statErr
	}

	return r.stat, nil
}

func (r *sendTestReader) Seek(offset int64, whence int) (int64, error) {
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

func (r *sendTestReader) Close() error {
	r.closeCalls++
	return r.closeErr
}

type failAfterRewindReader struct {
	*sendTestReader
	err     error
	rewound bool
}

func (r *failAfterRewindReader) Read(p []byte) (int, error) {
	if r.rewound {
		return 0, r.err
	}

	return r.sendTestReader.Read(p)
}

func (r *failAfterRewindReader) Seek(offset int64, whence int) (int64, error) {
	n, err := r.sendTestReader.Seek(offset, whence)
	if err == nil && offset == 0 && whence == io.SeekStart {
		r.rewound = true
	}

	return n, err
}

type sendTestStreamState struct {
	mu sync.Mutex

	readFrom    io.Reader
	readErr     error
	writeErr    error
	failWriteAt int
	writeCalls  int
	written     bytes.Buffer

	abortCh    chan struct{}
	abortOnce  sync.Once
	abortCalls int
	closeCalls int

	blockRead   bool
	readStarted chan struct{}

	blockWriteAt        int
	blockWriteAfterRead bool
	blockWrites         bool
	writeStarted        chan struct{}
	releaseWrite        chan struct{}

	readStartedOnce  sync.Once
	writeStartedOnce sync.Once
}

func newSendTestStream() (*mockStream, *sendTestStreamState) {
	state := &sendTestStreamState{
		abortCh: make(chan struct{}),
	}

	stream := &mockStream{
		readFunc:  state.read,
		writeFunc: state.write,
		closeFunc: state.close,
		abortFunc: state.abort,
	}

	return stream, state
}

func (s *sendTestStreamState) read(p []byte) (int, error) {
	s.mu.Lock()
	readFrom := s.readFrom
	readErr := s.readErr
	blockRead := s.blockRead
	readStarted := s.readStarted
	s.mu.Unlock()

	if blockRead {
		if readStarted != nil {
			s.readStartedOnce.Do(func() {
				close(readStarted)
			})
		}

		<-s.abortCh
		return 0, errSendTestStreamAborted
	}

	if readErr != nil {
		return 0, readErr
	}

	if readFrom == nil {
		return 0, io.EOF
	}

	n, err := readFrom.Read(p)

	if s.blockWriteAfterRead {
		if remaining, ok := readFrom.(interface{ Len() int }); ok && remaining.Len() == 0 {
			s.mu.Lock()
			s.blockWrites = true
			s.mu.Unlock()
		}
	}

	return n, err
}

func (s *sendTestStreamState) write(p []byte) (int, error) {
	s.mu.Lock()
	s.writeCalls++

	writeCalls := s.writeCalls
	writeErr := s.writeErr
	failWriteAt := s.failWriteAt
	blockWriteAt := s.blockWriteAt
	blockWrites := s.blockWrites
	writeStarted := s.writeStarted
	releaseWrite := s.releaseWrite
	s.mu.Unlock()

	if failWriteAt > 0 && writeCalls >= failWriteAt {
		if writeErr != nil {
			return 0, writeErr
		}

		return 0, errors.New("mock stream write failed")
	}

	if failWriteAt == 0 && writeErr != nil {
		return 0, writeErr
	}

	if blockWrites || (blockWriteAt > 0 && writeCalls >= blockWriteAt) {
		if writeStarted != nil {
			s.writeStartedOnce.Do(func() {
				close(writeStarted)
			})
		}

		select {
		case <-s.abortCh:
			return 0, errSendTestStreamAborted
		case <-releaseWrite:
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.written.Write(p)
}

func (s *sendTestStreamState) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closeCalls++

	return nil
}

func (s *sendTestStreamState) abort() {
	s.abortOnce.Do(func() {
		close(s.abortCh)
	})

	s.mu.Lock()
	s.abortCalls++
	s.mu.Unlock()
}

func (s *sendTestStreamState) stats() (
	writeCalls int,
	abortCalls int,
	closeCalls int,
	written []byte,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.writeCalls, s.abortCalls, s.closeCalls, append([]byte(nil), s.written.Bytes()...)
}

func newSendTestResponseReader(
	t *testing.T,
	response *models.TransferResponse,
) *bytes.Reader {
	t.Helper()

	var buf bytes.Buffer
	require.NoError(t, writeMessage(&buf, response))

	return bytes.NewReader(buf.Bytes())
}

func sendTestAddr() net.Addr {
	return &net.UDPAddr{
		IP:   net.ParseIP("192.0.2.10"),
		Port: 12345,
	}
}

func newTestSender(reader file.Reader) *send {
	stream, _ := newSendTestStream()

	return newSendTransfer(
		context.Background(),
		reader,
		stream,
		sendTestAddr(),
		testAcceptWaitTimeout,
	)
}

func newTestSenderWithStream(
	ctx context.Context,
	reader file.Reader,
	stream TransferStream,
	timeout time.Duration,
) *send {
	return newSendTransfer(
		ctx,
		reader,
		stream,
		sendTestAddr(),
		timeout,
	)
}

func awaitSendTestSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for signal")
	}
}

func awaitSendTestResult(t *testing.T, ch <-chan TransferResult) TransferResult {
	t.Helper()

	select {
	case result := <-ch:
		return result
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for send result")
		return nil
	}
}

func awaitSendTestSignalOrResult(
	t *testing.T,
	signal <-chan struct{},
	result <-chan TransferResult,
) {
	t.Helper()

	select {
	case <-signal:
		return
	case got := <-result:
		sendRes, ok := got.(*sendResult)
		if ok && sendRes.Err != nil {
			t.Fatalf("send exited before reaching expected operation: %v", sendRes.Err)
		}

		t.Fatalf("send exited before reaching expected operation: %#v", got)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for expected operation")
	}
}

func requireSendResult(t *testing.T, result TransferResult) *sendResult {
	t.Helper()

	sendRes, ok := result.(*sendResult)
	require.True(t, ok)
	require.NotNil(t, sendRes)

	return sendRes
}

func TestDetectMIME(t *testing.T) {
	t.Parallel()

	t.Run("detects PNG and rewinds reader", func(t *testing.T) {
		t.Parallel()

		data := []byte{
			0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A,
		}
		reader := newSendTestReader("picture.png", data)
		sender := newTestSender(reader)

		mimeType, err := sender.detectMIME()

		require.NoError(t, err)
		assert.Equal(t, "image/png", mimeType)
		assert.Zero(t, reader.pos)
	})

	t.Run("handles files smaller than 512 bytes", func(t *testing.T) {
		t.Parallel()

		reader := newSendTestReader("hello.txt", []byte("hello"))
		sender := newTestSender(reader)

		mimeType, err := sender.detectMIME()

		require.NoError(t, err)
		assert.Equal(t, "text/plain; charset=utf-8", mimeType)
		assert.Zero(t, reader.pos)
	})

	t.Run("handles empty file", func(t *testing.T) {
		t.Parallel()

		reader := newSendTestReader("empty", nil)
		sender := newTestSender(reader)

		mimeType, err := sender.detectMIME()

		require.NoError(t, err)
		assert.Equal(t, "application/octet-stream", mimeType)
		assert.Zero(t, reader.pos)
	})

	t.Run("propagates read error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("read failed")
		reader := newSendTestReader("picture.png", []byte("data"))
		reader.readErr = expectedErr
		sender := newTestSender(reader)

		_, err := sender.detectMIME()

		require.ErrorIs(t, err, expectedErr)
	})

	t.Run("propagates seek error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("seek failed")
		reader := newSendTestReader("hello.txt", []byte("hello"))
		reader.seekErr = expectedErr
		sender := newTestSender(reader)

		_, err := sender.detectMIME()

		require.ErrorIs(t, err, expectedErr)
	})
}

func TestInitRequest(t *testing.T) {
	t.Parallel()

	t.Run("initializes request and rewinds reader", func(t *testing.T) {
		t.Parallel()

		data := []byte{
			0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A,
		}
		reader := newSendTestReader("picture.png", data)
		sender := newTestSender(reader)

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
		assert.Equal(t, reader.stat.ModTime(), request.GetModifiedAt().AsTime())
		assert.Zero(t, reader.pos)
	})

	t.Run("rejects invalid state", func(t *testing.T) {
		t.Parallel()

		sender := newTestSender(newSendTestReader("report.txt", []byte("data")))
		sender.state = sendRequest

		request, err := sender.initRequest()

		require.Error(t, err)
		assert.Nil(t, request)
	})

	t.Run("propagates stat error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stat failed")
		reader := newSendTestReader("picture.png", []byte("data"))
		reader.statErr = expectedErr
		sender := newTestSender(reader)

		request, err := sender.initRequest()

		require.ErrorIs(t, err, expectedErr)
		assert.Nil(t, request)
	})

	t.Run("propagates MIME detection error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("read failed")
		reader := newSendTestReader("picture.png", []byte("data"))
		reader.readErr = expectedErr
		sender := newTestSender(reader)

		request, err := sender.initRequest()

		require.ErrorIs(t, err, expectedErr)
		assert.Nil(t, request)
	})

	t.Run("rejects invalid sender address", func(t *testing.T) {
		t.Parallel()

		reader := newSendTestReader("picture.png", []byte("data"))
		invalidAddr := &net.IPAddr{
			IP: net.ParseIP("192.0.2.10"),
		}
		stream, _ := newSendTestStream()

		sender := newSendTransfer(
			context.Background(),
			reader,
			stream,
			invalidAddr,
			testAcceptWaitTimeout,
		)

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

		sender := newTestSender(newSendTestReader("hello.txt", []byte("hello")))

		err := sender.sendTransferRequest()

		require.Error(t, err)
	})

	t.Run("propagates stream write error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream write failed")
		stream, state := newSendTestStream()
		state.writeErr = expectedErr

		sender := newTestSenderWithStream(
			context.Background(),
			newSendTestReader("hello.txt", []byte("hello")),
			stream,
			testAcceptWaitTimeout,
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

		stream, state := newSendTestStream()
		sender := newTestSenderWithStream(
			context.Background(),
			newSendTestReader("hello.txt", []byte("hello")),
			stream,
			testAcceptWaitTimeout,
		)
		sender.state = sendRequest
		sender.request = &models.TransferRequest{
			Filename: "hello.txt",
			FileSize: 5,
		}

		err := sender.sendTransferRequest()

		require.NoError(t, err)

		_, _, _, written := state.stats()
		received := &models.TransferRequest{}
		err = readMessage(bytes.NewReader(written), received)

		require.NoError(t, err)
		assert.Equal(t, sender.request.GetFilename(), received.GetFilename())
		assert.Equal(t, sender.request.GetFileSize(), received.GetFileSize())
	})
}

func TestRecieveTransferResponse(t *testing.T) {
	t.Parallel()

	t.Run("rejects invalid state", func(t *testing.T) {
		t.Parallel()

		sender := newTestSender(newSendTestReader("hello.txt", []byte("hello")))
		sender.response = &models.TransferResponse{}

		err := sender.recieveTransferResponse()

		require.Error(t, err)
	})

	t.Run("rejects uninitialized response", func(t *testing.T) {
		t.Parallel()

		sender := newTestSender(newSendTestReader("hello.txt", []byte("hello")))
		sender.state = sendWaitingResponse
		sender.response = nil

		err := sender.recieveTransferResponse()

		require.Error(t, err)
	})

	t.Run("reads accepted response", func(t *testing.T) {
		t.Parallel()

		responseReader := newSendTestResponseReader(
			t,
			&models.TransferResponse{Accepted: true},
		)
		stream, state := newSendTestStream()
		state.readFrom = responseReader

		sender := newTestSenderWithStream(
			context.Background(),
			newSendTestReader("hello.txt", []byte("hello")),
			stream,
			testAcceptWaitTimeout,
		)
		sender.state = sendWaitingResponse

		err := sender.recieveTransferResponse()

		require.NoError(t, err)
		assert.True(t, sender.response.GetAccepted())

		_, abortCalls, _, _ := state.stats()
		assert.Zero(t, abortCalls)
	})

	t.Run("reads rejection reason", func(t *testing.T) {
		t.Parallel()

		reason := "destination already exists"
		responseReader := newSendTestResponseReader(
			t,
			&models.TransferResponse{
				Accepted:     false,
				RejectReason: &reason,
			},
		)
		stream, state := newSendTestStream()
		state.readFrom = responseReader

		sender := newTestSenderWithStream(
			context.Background(),
			newSendTestReader("hello.txt", []byte("hello")),
			stream,
			testAcceptWaitTimeout,
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
		stream, state := newSendTestStream()
		state.readErr = expectedErr

		sender := newTestSenderWithStream(
			context.Background(),
			newSendTestReader("hello.txt", []byte("hello")),
			stream,
			testAcceptWaitTimeout,
		)
		sender.state = sendWaitingResponse

		err := sender.recieveTransferResponse()

		require.ErrorIs(t, err, expectedErr)
	})

	t.Run("aborts stream when approval times out", func(t *testing.T) {
		t.Parallel()

		stream, state := newSendTestStream()
		state.blockRead = true
		state.readStarted = make(chan struct{})

		sender := newTestSenderWithStream(
			context.Background(),
			newSendTestReader("hello.txt", []byte("hello")),
			stream,
			30*time.Millisecond,
		)
		sender.state = sendWaitingResponse

		errCh := make(chan error, 1)
		go func() {
			errCh <- sender.recieveTransferResponse()
		}()

		awaitSendTestSignal(t, state.readStarted)

		var err error
		select {
		case err = <-errCh:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for approval timeout")
		}

		require.ErrorIs(t, err, context.DeadlineExceeded)

		_, abortCalls, _, _ := state.stats()
		assert.Equal(t, 1, abortCalls)
	})

	t.Run("aborts stream when parent context is cancelled", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stream, state := newSendTestStream()
		state.blockRead = true
		state.readStarted = make(chan struct{})

		sender := newTestSenderWithStream(
			ctx,
			newSendTestReader("hello.txt", []byte("hello")),
			stream,
			time.Second,
		)
		sender.state = sendWaitingResponse

		errCh := make(chan error, 1)
		go func() {
			errCh <- sender.recieveTransferResponse()
		}()

		awaitSendTestSignal(t, state.readStarted)
		cancel()

		var err error
		select {
		case err = <-errCh:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for parent cancellation")
		}

		require.ErrorIs(t, err, context.Canceled)

		_, abortCalls, _, _ := state.stats()
		assert.GreaterOrEqual(t, abortCalls, 1)
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
		reader := newSendTestReader("binary.dat", data)
		stream, state := newSendTestStream()
		sender := newTestSenderWithStream(
			context.Background(),
			reader,
			stream,
			testAcceptWaitTimeout,
		)
		sender.state = sendSending

		err := sender.sendData()

		require.NoError(t, err)

		writeCalls, _, _, written := state.stats()
		assert.Equal(t, data, written)
		assert.Greater(t, writeCalls, 1)
		assert.Equal(t, len(data), reader.pos)
	})

	t.Run("empty file sends no data", func(t *testing.T) {
		t.Parallel()

		reader := newSendTestReader("empty", nil)
		stream, state := newSendTestStream()
		sender := newTestSenderWithStream(
			context.Background(),
			reader,
			stream,
			testAcceptWaitTimeout,
		)
		sender.state = sendSending

		err := sender.sendData()

		require.NoError(t, err)

		writeCalls, _, _, written := state.stats()
		assert.Zero(t, writeCalls)
		assert.Empty(t, written)
	})

	t.Run("rejects invalid state", func(t *testing.T) {
		t.Parallel()

		sender := newTestSender(newSendTestReader("hello.txt", []byte("hello")))

		err := sender.sendData()

		require.Error(t, err)
	})

	t.Run("propagates reader error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("file read failed")
		reader := newSendTestReader("hello.txt", []byte("hello"))
		reader.readErr = expectedErr

		sender := newTestSenderWithStream(
			context.Background(),
			reader,
			func() TransferStream {
				stream, _ := newSendTestStream()
				return stream
			}(),
			testAcceptWaitTimeout,
		)
		sender.state = sendSending

		err := sender.sendData()

		require.ErrorIs(t, err, expectedErr)
	})

	t.Run("propagates stream write error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream write failed")
		reader := newSendTestReader("hello.txt", []byte("hello"))
		stream, state := newSendTestStream()
		state.writeErr = expectedErr

		sender := newTestSenderWithStream(
			context.Background(),
			reader,
			stream,
			testAcceptWaitTimeout,
		)
		sender.state = sendSending

		err := sender.sendData()

		require.ErrorIs(t, err, expectedErr)
	})
}

func TestTransferSendRun(t *testing.T) {
	t.Parallel()

	t.Run("sends request and file when accepted", func(t *testing.T) {
		t.Parallel()

		fileData := []byte("hello file contents")
		reader := newSendTestReader("hello.txt", fileData)
		responseReader := newSendTestResponseReader(
			t,
			&models.TransferResponse{Accepted: true},
		)
		stream, state := newSendTestStream()
		state.readFrom = responseReader

		sender := newTestSenderWithStream(
			context.Background(),
			reader,
			stream,
			testAcceptWaitTimeout,
		)
		sender.setId("test")

		result := requireSendResult(t, sender.run())

		require.NoError(t, result.Err)
		assert.True(t, result.Accepted)
		assert.Nil(t, result.RejectReason)
		assert.Equal(t, sendDone, sender.state)
		assert.Equal(t, len(fileData), reader.pos)
		assert.Equal(t, 1, reader.closeCalls)

		_, _, closeCalls, written := state.stats()
		assert.Equal(t, 1, closeCalls)

		out := bytes.NewReader(written)
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
		reader := newSendTestReader("hello.txt", []byte("hello"))
		responseReader := newSendTestResponseReader(
			t,
			&models.TransferResponse{
				Accepted:     false,
				RejectReason: &reason,
			},
		)
		stream, state := newSendTestStream()
		state.readFrom = responseReader

		sender := newTestSenderWithStream(
			context.Background(),
			reader,
			stream,
			testAcceptWaitTimeout,
		)
		sender.setId("test")

		result := requireSendResult(t, sender.run())

		require.NoError(t, result.Err)
		assert.False(t, result.Accepted)
		assert.Equal(t, sendRejected, sender.state)
		require.NotNil(t, result.RejectReason)
		assert.Equal(t, reason, *result.RejectReason)

		_, _, _, written := state.stats()
		out := bytes.NewReader(written)
		received := &models.TransferRequest{}

		require.NoError(t, readMessage(out, received))
		assert.Equal(t, "hello.txt", received.GetFilename())
		assert.Zero(t, out.Len())
		assert.Zero(t, reader.pos)
	})

	t.Run("stops when request initialization fails", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stat failed")
		reader := newSendTestReader("hello.txt", []byte("hello"))
		reader.statErr = expectedErr
		stream, state := newSendTestStream()

		sender := newTestSenderWithStream(
			context.Background(),
			reader,
			stream,
			testAcceptWaitTimeout,
		)
		sender.setId("test")

		result := requireSendResult(t, sender.run())

		require.ErrorIs(t, result.Err, expectedErr)
		assert.Equal(t, sendError, sender.state)
		assert.Nil(t, sender.request)

		writeCalls, _, closeCalls, _ := state.stats()
		assert.Zero(t, writeCalls)
		assert.Equal(t, 1, closeCalls)
		assert.Equal(t, 1, reader.closeCalls)
	})

	t.Run("stops when sending request fails", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream write failed")
		reader := newSendTestReader("hello.txt", []byte("hello"))
		stream, state := newSendTestStream()
		state.writeErr = expectedErr

		sender := newTestSenderWithStream(
			context.Background(),
			reader,
			stream,
			testAcceptWaitTimeout,
		)
		sender.setId("test")

		result := requireSendResult(t, sender.run())

		require.ErrorIs(t, result.Err, expectedErr)
		assert.Equal(t, sendError, sender.state)
		assert.NotNil(t, sender.request)
	})

	t.Run("stops when receiving response fails", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream read failed")
		reader := newSendTestReader("hello.txt", []byte("hello"))
		stream, state := newSendTestStream()
		state.readErr = expectedErr

		sender := newTestSenderWithStream(
			context.Background(),
			reader,
			stream,
			testAcceptWaitTimeout,
		)
		sender.setId("test")

		result := requireSendResult(t, sender.run())

		require.ErrorIs(t, result.Err, expectedErr)
		assert.Equal(t, sendError, sender.state)
	})

	t.Run("stops when reading file data fails", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("file read failed")
		reader := &failAfterRewindReader{
			sendTestReader: newSendTestReader("hello.txt", []byte("hello")),
			err:            expectedErr,
		}
		responseReader := newSendTestResponseReader(
			t,
			&models.TransferResponse{Accepted: true},
		)
		stream, state := newSendTestStream()
		state.readFrom = responseReader

		sender := newTestSenderWithStream(
			context.Background(),
			reader,
			stream,
			testAcceptWaitTimeout,
		)
		sender.setId("test")

		result := requireSendResult(t, sender.run())

		require.ErrorIs(t, result.Err, expectedErr)
		assert.True(t, result.Accepted)
		assert.Equal(t, sendError, sender.state)
	})

	t.Run("stops when writing file data fails", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("file write failed")
		reader := newSendTestReader("hello.txt", []byte("hello"))
		responseReader := newSendTestResponseReader(
			t,
			&models.TransferResponse{Accepted: true},
		)
		stream, state := newSendTestStream()
		state.readFrom = responseReader
		state.writeErr = expectedErr
		state.failWriteAt = 3

		sender := newTestSenderWithStream(
			context.Background(),
			reader,
			stream,
			testAcceptWaitTimeout,
		)
		sender.setId("test")

		result := requireSendResult(t, sender.run())

		require.ErrorIs(t, result.Err, expectedErr)
		assert.True(t, result.Accepted)
		assert.Equal(t, sendError, sender.state)
	})

	t.Run("times out while waiting for approval", func(t *testing.T) {
		t.Parallel()

		reader := newSendTestReader("hello.txt", []byte("hello"))
		stream, state := newSendTestStream()
		state.blockRead = true
		state.readStarted = make(chan struct{})

		sender := newTestSenderWithStream(
			context.Background(),
			reader,
			stream,
			30*time.Millisecond,
		)
		sender.setId("test")

		resultCh := make(chan TransferResult, 1)
		go func() {
			resultCh <- sender.run()
		}()

		awaitSendTestSignalOrResult(t, state.readStarted, resultCh)

		result := requireSendResult(t, awaitSendTestResult(t, resultCh))

		require.ErrorIs(t, result.Err, context.DeadlineExceeded)
		assert.Equal(t, sendError, sender.state)

		_, abortCalls, closeCalls, _ := state.stats()
		assert.Equal(t, 1, abortCalls)
		assert.Equal(t, 1, closeCalls)
		assert.Equal(t, 1, reader.closeCalls)
	})

	t.Run("cancels while waiting for approval when parent context is cancelled", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		reader := newSendTestReader("hello.txt", []byte("hello"))
		stream, state := newSendTestStream()
		state.blockRead = true
		state.readStarted = make(chan struct{})

		sender := newTestSenderWithStream(
			ctx,
			reader,
			stream,
			time.Second,
		)
		sender.setId("test")

		resultCh := make(chan TransferResult, 1)
		go func() {
			resultCh <- sender.run()
		}()

		awaitSendTestSignalOrResult(t, state.readStarted, resultCh)
		cancel()

		result := requireSendResult(t, awaitSendTestResult(t, resultCh))

		require.ErrorIs(t, result.Err, context.Canceled)
		assert.Equal(t, sendError, sender.state)

		_, abortCalls, closeCalls, _ := state.stats()
		assert.GreaterOrEqual(t, abortCalls, 1)
		assert.Equal(t, 1, closeCalls)
		assert.Equal(t, 1, reader.closeCalls)
	})

	t.Run("approval timeout does not apply to file sending", func(t *testing.T) {
		t.Parallel()

		approvalTimeout := 30 * time.Millisecond
		reader := newSendTestReader("hello.txt", []byte("hello"))
		responseReader := newSendTestResponseReader(
			t,
			&models.TransferResponse{Accepted: true},
		)
		stream, state := newSendTestStream()
		state.readFrom = responseReader
		state.blockWriteAfterRead = true
		state.writeStarted = make(chan struct{})
		state.releaseWrite = make(chan struct{})

		sender := newTestSenderWithStream(
			context.Background(),
			reader,
			stream,
			approvalTimeout,
		)
		sender.setId("test")

		resultCh := make(chan TransferResult, 1)
		go func() {
			resultCh <- sender.run()
		}()

		awaitSendTestSignalOrResult(t, state.writeStarted, resultCh)

		select {
		case result := <-resultCh:
			require.FailNow(t, "send completed before blocked write was released", "result: %#v", result)
		case <-time.After(approvalTimeout * 3):
		}

		_, abortCalls, _, _ := state.stats()
		assert.Zero(t, abortCalls)

		close(state.releaseWrite)

		result := requireSendResult(t, awaitSendTestResult(t, resultCh))

		require.NoError(t, result.Err)
		assert.True(t, result.Accepted)
		assert.Equal(t, sendDone, sender.state)
	})

	t.Run("cancels while sending file data when parent context is cancelled", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		reader := newSendTestReader("hello.txt", []byte("hello"))
		responseReader := newSendTestResponseReader(
			t,
			&models.TransferResponse{Accepted: true},
		)
		stream, state := newSendTestStream()
		state.readFrom = responseReader
		state.blockWriteAfterRead = true
		state.writeStarted = make(chan struct{})

		sender := newTestSenderWithStream(
			ctx,
			reader,
			stream,
			time.Second,
		)
		sender.setId("test")

		resultCh := make(chan TransferResult, 1)
		go func() {
			resultCh <- sender.run()
		}()

		awaitSendTestSignalOrResult(t, state.writeStarted, resultCh)
		cancel()

		result := requireSendResult(t, awaitSendTestResult(t, resultCh))

		require.ErrorIs(t, result.Err, context.Canceled)
		assert.True(t, result.Accepted)
		assert.Equal(t, sendError, sender.state)

		_, abortCalls, closeCalls, _ := state.stats()
		assert.GreaterOrEqual(t, abortCalls, 1)
		assert.Equal(t, 1, closeCalls)
		assert.Equal(t, 1, reader.closeCalls)
	})

	t.Run("does not allow second call", func(t *testing.T) {
		t.Parallel()

		reader := newSendTestReader("hello.txt", []byte("hello"))
		responseReader := newSendTestResponseReader(
			t,
			&models.TransferResponse{Accepted: true},
		)
		stream, state := newSendTestStream()
		state.readFrom = responseReader

		sender := newTestSenderWithStream(
			context.Background(),
			reader,
			stream,
			testAcceptWaitTimeout,
		)
		sender.setId("test")

		firstResult := requireSendResult(t, sender.run())

		require.NoError(t, firstResult.Err)
		assert.True(t, firstResult.Accepted)
		assert.Equal(t, sendDone, sender.state)

		readsAfterFirstRun := reader.pos
		writeCallsAfterFirstRun, abortCallsAfterFirstRun, closeCallsAfterFirstRun, writtenAfterFirstRun := state.stats()

		secondResult := requireSendResult(t, sender.run())

		require.Error(t, secondResult.Err)
		assert.ErrorContains(t, secondResult.Err, "already called")
		assert.Equal(t, sendError, sender.state)

		assert.Equal(t, readsAfterFirstRun, reader.pos)

		writeCallsAfterSecondRun, abortCallsAfterSecondRun, closeCallsAfterSecondRun, writtenAfterSecondRun := state.stats()
		assert.Equal(t, writeCallsAfterFirstRun, writeCallsAfterSecondRun)
		assert.Equal(t, abortCallsAfterFirstRun, abortCallsAfterSecondRun)
		assert.Equal(t, closeCallsAfterFirstRun, closeCallsAfterSecondRun)
		assert.Equal(t, writtenAfterFirstRun, writtenAfterSecondRun)
	})
}
