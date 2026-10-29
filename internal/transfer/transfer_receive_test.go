package transfer

import (
	"airlink/internal/transfer/models"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type receiveTestStream struct {
	reader     io.Reader
	readErr    error
	writeErr   error
	written    bytes.Buffer
	readCalls  int
	writeCalls int
}

func (s *receiveTestStream) Read(p []byte) (int, error) {
	s.readCalls++

	if s.readErr != nil {
		return 0, s.readErr
	}

	if s.reader == nil {
		return 0, io.EOF
	}

	return s.reader.Read(p)
}

func (s *receiveTestStream) Write(p []byte) (int, error) {
	s.writeCalls++

	if s.writeErr != nil {
		return 0, s.writeErr
	}

	return s.written.Write(p)
}

func (s *receiveTestStream) Close() error {
	return nil
}

type limitedReadReader struct {
	reader    io.Reader
	chunkSize int
}

func (r *limitedReadReader) Read(p []byte) (int, error) {
	if len(p) > r.chunkSize {
		p = p[:r.chunkSize]
	}

	return r.reader.Read(p)
}

type singleReadResultReader struct {
	data []byte
	err  error
	done bool
}

func (r *singleReadResultReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}

	r.done = true
	n := copy(p, r.data)
	return n, r.err
}

type errorAfterReader struct {
	reader io.Reader
	err    error
}

func (r *errorAfterReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n == 0 && errors.Is(err, io.EOF) {
		return 0, r.err
	}

	return n, err
}

func newReceiveInput(
	t *testing.T,
	request *models.TransferRequest,
	fileData []byte,
) *bytes.Reader {
	t.Helper()

	var buf bytes.Buffer
	require.NoError(t, writeMessage(&buf, request))

	_, err := buf.Write(fileData)
	require.NoError(t, err)

	return bytes.NewReader(buf.Bytes())
}

func newInvalidReceiveMessageReader() *bytes.Reader {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, 1)

	return bytes.NewReader(append(header, 0xff))
}

func TestNewTransferReceive(t *testing.T) {
	t.Parallel()

	t.Run("initializes receiver with stream", func(t *testing.T) {
		t.Parallel()

		stream := &receiveTestStream{}

		receiver := newTransferReceive(stream)

		require.NotNil(t, receiver)
		assert.Equal(t, receiveInit, receiver.state)
		assert.Same(t, stream, receiver.stream)
		require.NotNil(t, receiver.request)
		assert.Nil(t, receiver.response)
	})

	t.Run("accepts nil stream", func(t *testing.T) {
		t.Parallel()

		receiver := newTransferReceive(nil)

		require.NotNil(t, receiver)
		assert.Equal(t, receiveInit, receiver.state)
		assert.Nil(t, receiver.stream)
		require.NotNil(t, receiver.request)
	})
}

func TestTransferReceiveRun(t *testing.T) {
	t.Parallel()

	t.Run("receives request and file data", func(t *testing.T) {
		t.Parallel()

		fileData := []byte("hello file contents")
		expected := &models.TransferRequest{
			Filename:  "hello.txt",
			Extension: ".txt",
			FileSize:  uint64(len(fileData)),
			Mime:      "text/plain",
			SenderIp:  "192.0.2.10",
		}

		stream := &receiveTestStream{
			reader: newReceiveInput(t, expected, fileData),
		}

		receiver := newTransferReceive(stream)

		result := receiver.run()

		require.NotNil(t, result)
		require.NoError(t, result.Err)

		assert.Equal(t, receiveDone, receiver.state)

		require.NotNil(t, result.Request)
		assert.Equal(t, expected.GetFilename(), result.Request.GetFilename())
		assert.Equal(t, expected.GetFileSize(), result.Request.GetFileSize())
		assert.Equal(t, expected.GetMime(), result.Request.GetMime())

		require.NotNil(t, result.Response)
		assert.True(t, result.Response.GetAccepted())

		require.NotEmpty(t, result.DestinationPath)
		assert.True(t, filepath.IsAbs(result.DestinationPath))
		assert.Equal(t, "hello.txt", filepath.Base(result.DestinationPath))

		// The response should have been written to the stream.
		assert.NotZero(t, stream.written.Len())

		writtenResponse := &models.TransferResponse{}
		err := readMessage(bytes.NewReader(stream.written.Bytes()), writtenResponse)

		require.NoError(t, err)
		assert.True(t, writtenResponse.GetAccepted())
	})

	t.Run("transitions to error when reading request fails", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream read failed")
		stream := &receiveTestStream{
			readErr: expectedErr,
		}

		receiver := newTransferReceive(stream)

		result := receiver.run()

		require.NotNil(t, result)
		require.ErrorIs(t, result.Err, expectedErr)
		assert.Equal(t, receiveError, receiver.state)
		assert.Nil(t, result.Response)
		assert.Zero(t, stream.writeCalls)
	})

	t.Run("transitions to error when request protobuf is invalid", func(t *testing.T) {
		t.Parallel()

		stream := &receiveTestStream{
			reader: newInvalidReceiveMessageReader(),
		}

		receiver := newTransferReceive(stream)

		result := receiver.run()

		require.NotNil(t, result)
		require.Error(t, result.Err)
		assert.Equal(t, receiveError, receiver.state)
		assert.Nil(t, result.Response)
		assert.Zero(t, stream.writeCalls)
	})

	t.Run("transitions to error when sending response fails", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream write failed")
		request := &models.TransferRequest{
			Filename: "hello.txt",
			FileSize: 5,
		}

		stream := &receiveTestStream{
			reader:   newReceiveInput(t, request, []byte("hello")),
			writeErr: expectedErr,
		}

		receiver := newTransferReceive(stream)

		result := receiver.run()

		require.NotNil(t, result)
		require.ErrorIs(t, result.Err, expectedErr)
		assert.Equal(t, receiveError, receiver.state)
		assert.NotNil(t, result.Request)
		assert.NotNil(t, result.Response)
	})

	t.Run("transitions to error when file read fails", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("file read failed")
		request := &models.TransferRequest{
			Filename: "hello.txt",
			FileSize: 5,
		}

		var requestBuf bytes.Buffer
		require.NoError(t, writeMessage(&requestBuf, request))

		stream := &receiveTestStream{
			reader: &errorAfterReader{
				reader: bytes.NewReader(requestBuf.Bytes()),
				err:    expectedErr,
			},
		}

		receiver := newTransferReceive(stream)

		result := receiver.run()

		require.NotNil(t, result)
		require.ErrorIs(t, result.Err, expectedErr)
		assert.Equal(t, receiveError, receiver.state)
		assert.NotNil(t, result.Response)
	})

	t.Run("stops when already done", func(t *testing.T) {
		t.Parallel()

		receiver := &transferReceive{
			state: receiveDone,
			stream: &receiveTestStream{},
		}

		result := receiver.run()

		require.NotNil(t, result)
		require.NoError(t, result.Err)
		assert.Equal(t, receiveDone, receiver.state)
	})

	t.Run("handles invalid state", func(t *testing.T) {
		t.Parallel()

		receiver := &transferReceive{
			state:  receiveState("invalid"),
			stream: &receiveTestStream{},
		}

		result := receiver.run()

		require.NotNil(t, result)
		require.ErrorContains(t, result.Err, "invalid transfer receive state")
		assert.Equal(t, receiveError, receiver.state)
	})
}

func TestIngestRequest(t *testing.T) {
	t.Parallel()

	t.Run("reads and decodes request", func(t *testing.T) {
		t.Parallel()

		expected := &models.TransferRequest{
			Filename:  "report.pdf",
			Extension: ".pdf",
			FileSize:  1024,
			Mime:      "application/pdf",
		}

		stream := &receiveTestStream{
			reader: newReceiveInput(t, expected, nil),
		}

		receiver := &transferReceive{
			state:   receiveIngestingRequest,
			request: &models.TransferRequest{},
			stream:  stream,
		}

		err := receiver.ingestRequst()

		require.NoError(t, err)
		assert.Equal(t, expected.GetFilename(), receiver.request.GetFilename())
		assert.Equal(t, expected.GetExtension(), receiver.request.GetExtension())
		assert.Equal(t, expected.GetFileSize(), receiver.request.GetFileSize())
		assert.Equal(t, expected.GetMime(), receiver.request.GetMime())
	})

	t.Run("rejects invalid state", func(t *testing.T) {
		t.Parallel()

		stream := &receiveTestStream{}
		receiver := &transferReceive{
			state:   receiveInit,
			request: &models.TransferRequest{},
			stream:  stream,
		}

		err := receiver.ingestRequst()

		require.Error(t, err)
		assert.Zero(t, stream.readCalls)
	})

	t.Run("rejects nil request", func(t *testing.T) {
		t.Parallel()

		stream := &receiveTestStream{}
		receiver := &transferReceive{
			state:  receiveIngestingRequest,
			stream: stream,
		}

		err := receiver.ingestRequst()

		require.Error(t, err)
		assert.Zero(t, stream.readCalls)
	})

	t.Run("propagates stream read error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream read failed")

		receiver := &transferReceive{
			state:   receiveIngestingRequest,
			request: &models.TransferRequest{},
			stream:  &receiveTestStream{readErr: expectedErr},
		}

		err := receiver.ingestRequst()

		require.ErrorIs(t, err, expectedErr)
	})

	t.Run("rejects malformed protobuf", func(t *testing.T) {
		t.Parallel()

		receiver := &transferReceive{
			state:   receiveIngestingRequest,
			request: &models.TransferRequest{},
			stream: &receiveTestStream{
				reader: newInvalidReceiveMessageReader(),
			},
		}

		err := receiver.ingestRequst()

		require.Error(t, err)
	})
}

func TestProcessResponse(t *testing.T) {
	t.Parallel()

	t.Run("accepts request by default and resolves destination", func(t *testing.T) {
		t.Parallel()

		receiver := &transferReceive{
			state: receiveProcessingRequest,
			request: &models.TransferRequest{
				Filename: "hello.txt",
				FileSize: 5,
			},
		}

		response, destinationPath, err := receiver.processResponse()

		require.NoError(t, err)
		require.NotNil(t, response)
		assert.True(t, response.GetAccepted())
		assert.Empty(t, response.GetRejectReason())
		assert.True(t, filepath.IsAbs(destinationPath))
		assert.Equal(t, "hello.txt", filepath.Base(destinationPath))
	})

	t.Run("rejects invalid state", func(t *testing.T) {
		t.Parallel()

		receiver := &transferReceive{
			state: receiveInit,
		}

		response, destinationPath, err := receiver.processResponse()

		require.Error(t, err)
		assert.Nil(t, response)
		assert.Empty(t, destinationPath)
	})
}

func TestResolveDestination(t *testing.T) {
	t.Parallel()

	t.Run("returns absolute path", func(t *testing.T) {
		t.Parallel()

		receiver := newTransferReceive(&receiveTestStream{})

		destination, err := receiver.resolveDestination("report.txt")

		require.NoError(t, err)
		assert.True(t, filepath.IsAbs(destination))
		assert.Equal(t, "report.txt", filepath.Base(destination))

		info, err := os.Stat(filepath.Dir(destination))
		require.NoError(t, err)
		assert.True(t, info.IsDir())
	})

	t.Run("strips directory components from filename", func(t *testing.T) {
		t.Parallel()

		receiver := newTransferReceive(&receiveTestStream{})

		destination, err := receiver.resolveDestination("../outside.txt")
		expectedDir, err := filepath.Abs(defaultDestinationDir)

		require.NoError(t, err)
		assert.True(t, filepath.IsAbs(destination))
		assert.Equal(t, "outside.txt", filepath.Base(destination))
		require.NoError(t, err)
		assert.Equal(t, expectedDir, filepath.Dir(destination))
	})
}

func TestSendResponse(t *testing.T) {
	t.Parallel()

	t.Run("writes response successfully", func(t *testing.T) {
		t.Parallel()

		stream := &receiveTestStream{}
		receiver := &transferReceive{
			state:  receiveRespond,
			stream: stream,
			response: &models.TransferResponse{
				Accepted: true,
			},
		}

		err := receiver.sendResponse()

		require.NoError(t, err)
		assert.NotZero(t, stream.written.Len())

		received := &models.TransferResponse{}
		err = readMessage(bytes.NewReader(stream.written.Bytes()), received)

		require.NoError(t, err)
		assert.True(t, received.GetAccepted())
	})

	t.Run("rejects invalid state", func(t *testing.T) {
		t.Parallel()

		stream := &receiveTestStream{}
		receiver := &transferReceive{
			state:  receiveInit,
			stream: stream,
			response: &models.TransferResponse{
				Accepted: true,
			},
		}

		err := receiver.sendResponse()

		require.Error(t, err)
		assert.Zero(t, stream.writeCalls)
	})

	t.Run("propagates stream write error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream write failed")
		receiver := &transferReceive{
			state:    receiveRespond,
			stream:   &receiveTestStream{writeErr: expectedErr},
			response: &models.TransferResponse{Accepted: true},
		}

		err := receiver.sendResponse()

		require.ErrorIs(t, err, expectedErr)
	})
}

func TestReceiveData(t *testing.T) {
	t.Parallel()

	t.Run("receives data across multiple reads", func(t *testing.T) {
		t.Parallel()

		data := []byte("some binary file contents")
		stream := &receiveTestStream{
			reader: &limitedReadReader{
				reader:    bytes.NewReader(data),
				chunkSize: 3,
			},
		}

		receiver := &transferReceive{
			state: receiveReceiving,
			stream: stream,
			destinationPath: "/tmp/output.txt",
		}

		totalBytesRead, err := receiver.receiveData()

		require.NoError(t, err)
		assert.Equal(t, len(data), totalBytesRead)
		assert.Greater(t, stream.readCalls, 1)
	})

	t.Run("handles empty stream", func(t *testing.T) {
		t.Parallel()

		stream := &receiveTestStream{
			reader: bytes.NewReader(nil),
		}

		receiver := &transferReceive{
			state:  receiveReceiving,
			stream: stream,
		}

		totalBytesRead, err := receiver.receiveData()

		require.NoError(t, err)
		assert.Zero(t, totalBytesRead)
		assert.Equal(t, 1, stream.readCalls)
	})

	t.Run("rejects invalid state", func(t *testing.T) {
		t.Parallel()

		stream := &receiveTestStream{
			reader: bytes.NewReader([]byte("data")),
		}

		receiver := &transferReceive{
			state:  receiveInit,
			stream: stream,
		}

		totalBytesRead, err := receiver.receiveData()

		require.Error(t, err)
		assert.Zero(t, totalBytesRead)
		assert.Zero(t, stream.readCalls)
	})

	t.Run("propagates stream read error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream read failed")
		stream := &receiveTestStream{readErr: expectedErr}

		receiver := &transferReceive{
			state:  receiveReceiving,
			stream: stream,
		}

		totalBytesRead, err := receiver.receiveData()

		require.ErrorIs(t, err, expectedErr)
		assert.Zero(t, totalBytesRead)
	})

	t.Run("counts data returned together with EOF", func(t *testing.T) {
		t.Parallel()

		data := []byte("final bytes")
		stream := &receiveTestStream{
			reader: &singleReadResultReader{
				data: data,
				err:  io.EOF,
			},
		}

		receiver := &transferReceive{
			state:  receiveReceiving,
			stream: stream,
		}

		totalBytesRead, err := receiver.receiveData()

		require.NoError(t, err)
		assert.Equal(t, len(data), totalBytesRead)
	})

	t.Run("counts partial data when read fails", func(t *testing.T) {
		t.Parallel()

		data := []byte("partial data")
		expectedErr := errors.New("connection interrupted")

		stream := &receiveTestStream{
			reader: &singleReadResultReader{
				data: data,
				err:  expectedErr,
			},
		}

		receiver := &transferReceive{
			state:  receiveReceiving,
			stream: stream,
		}

		totalBytesRead, err := receiver.receiveData()

		require.ErrorIs(t, err, expectedErr)
		assert.Equal(t, len(data), totalBytesRead)
	})
}
