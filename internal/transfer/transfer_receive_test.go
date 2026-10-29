package transfer

import (
	"airlink/internal/transfer/models"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"airlink/internal/transfer/models"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
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

func newReceiveRequestReader(
	t *testing.T,
	request *models.TransferRequest,
) *bytes.Reader {
	t.Helper()

	var buf bytes.Buffer
	require.NoError(t, writeMessage(&buf, request))

	return bytes.NewReader(buf.Bytes())
}

func newInvalidReceiveMessageReader() *bytes.Reader {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, 1)

	return bytes.NewReader(append(header, 0xff))
}

func TestTransferReceiveRun(t *testing.T) {
	t.Parallel()

	t.Run("ingests request and processes it", func(t *testing.T) {
		t.Parallel()

		expected := &models.TransferRequest{
			Filename:  "hello.txt",
			Extension: ".txt",
			FileSize:  5,
			Mime:      "text/plain",
			SenderIp:  "192.0.2.10",
		}

		stream := &receiveTestStream{
			reader: newReceiveRequestReader(t, expected),
		}

		expected := &models.TransferRequest{
			Filename:  "hello.txt",
			Extension: ".txt",
			FileSize:  5,
			Mime:      "text/plain",
			SenderIp:  "192.0.2.10",
		}

		stream := &receiveTestStream{
			reader: newReceiveRequestReader(t, expected),
		}

		receiver := &transferReceive{
			state:  receiveInit,
			stream: stream,
			state:  receiveInit,
			stream: stream,
		}

		result := receiver.run()

		require.NotNil(t, result)
		require.NoError(t, result.Err)

		assert.Equal(t, receiveRespond, receiver.state)
		assert.True(t, receiver.consumed)

		require.NotNil(t, receiver.request)
		require.NotNil(t, result.Request)
		require.NotNil(t, result.Response)

		assert.Equal(t, expected.GetFilename(), result.Request.GetFilename())
		assert.Equal(t, expected.GetExtension(), result.Request.GetExtension())
		assert.Equal(t, expected.GetFileSize(), result.Request.GetFileSize())
		assert.Equal(t, expected.GetMime(), result.Request.GetMime())
		assert.Equal(t, expected.GetSenderIp(), result.Request.GetSenderIp())

		// processResponse currently accepts every request.
		assert.True(t, result.Response.GetAccepted())
		assert.Empty(t, result.Response.GetRejectReason())
		assert.Zero(t, stream.writeCalls)
	})

	t.Run("transitions to error when stream read fails", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream read failed")

		stream := &receiveTestStream{
			readErr: expectedErr,
		}

		receiver := &transferReceive{
			state:  receiveInit,
			stream: stream,
		}

		result := receiver.run()

		require.NotNil(t, result)
		require.ErrorIs(t, result.Err, expectedErr)

		assert.Equal(t, receiveError, receiver.state)
		assert.True(t, receiver.consumed)
		assert.Nil(t, result.Request)
		assert.Nil(t, result.Response)
	})

	t.Run("transitions to error when protobuf is invalid", func(t *testing.T) {
		t.Parallel()

		stream := &receiveTestStream{
			reader: newInvalidReceiveMessageReader(),
		}

		receiver := &transferReceive{
			state:  receiveInit,
			stream: stream,
		}

		result := receiver.run()

		require.NotNil(t, result)
		require.Error(t, result.Err)

		assert.Equal(t, receiveError, receiver.state)
		assert.Nil(t, result.Request)
		assert.Nil(t, result.Response)
	})

	t.Run("rejects a second invocation", func(t *testing.T) {
		t.Parallel()

		stream := &receiveTestStream{
			reader: newReceiveRequestReader(t, &models.TransferRequest{
				Filename: "hello.txt",
				FileSize: 5,
			}),
		}

		receiver := &transferReceive{
			state:  receiveInit,
			stream: stream,
			state:  receiveInit,
			stream: stream,
		}

		firstResult := receiver.run()


		require.NoError(t, firstResult.Err)
		assert.Equal(t, receiveRespond, receiver.state)

		readCallsAfterFirstRun := stream.readCalls

		secondResult := receiver.run()

		require.NotNil(t, secondResult)
		require.ErrorContains(t, secondResult.Err, "already called")
		assert.Equal(t, receiveError, receiver.state)
		assert.Equal(t, readCallsAfterFirstRun, stream.readCalls)
		assert.Equal(t, readCallsAfterFirstRun, stream.readCalls)
	})

	t.Run("stops when already done", func(t *testing.T) {
		t.Parallel()

		receiver := &transferReceive{
			state:  receiveDone,
			stream: &receiveTestStream{},
		}

		result := receiver.run()

		require.NotNil(t, result)
		require.NoError(t, result.Err)
		assert.Equal(t, receiveDone, receiver.state)
		assert.True(t, receiver.consumed)
	})

	t.Run("handles invalid state", func(t *testing.T) {
		t.Parallel()

		receiver := &transferReceive{
			state:  receiveState("invalid"),
			stream: &receiveTestStream{},
		}

		request, err := receiver.ingestRequst()

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
			reader: newReceiveRequestReader(t, expected),
		}

		receiver := &transferReceive{
			state:  receiveIngestingRequest,
			stream: stream,
		}

		request, err := receiver.ingestRequst()

		require.NoError(t, err)
		require.NotNil(t, request)

		assert.Equal(t, expected.GetFilename(), request.GetFilename())
		assert.Equal(t, expected.GetExtension(), request.GetExtension())
		assert.Equal(t, expected.GetFileSize(), request.GetFileSize())
		assert.Equal(t, expected.GetMime(), request.GetMime())
	})

	t.Run("rejects invalid state", func(t *testing.T) {
		t.Parallel()

		stream := &receiveTestStream{}

		receiver := &transferReceive{
			state:  receiveInit,
			stream: stream,
		}

		request, err := receiver.ingestRequst()

		require.Error(t, err)
		assert.Nil(t, request)
		assert.Zero(t, stream.readCalls)
	})

	t.Run("propagates stream read error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream read failed")

		receiver := &transferReceive{
			state: receiveIngestingRequest,
			stream: &receiveTestStream{
				readErr: expectedErr,
			},
		}

		request, err := receiver.ingestRequst()

		require.ErrorIs(t, err, expectedErr)
		assert.Nil(t, request)
	})

	t.Run("rejects malformed protobuf", func(t *testing.T) {
		t.Parallel()

		receiver := &transferReceive{
			state: receiveIngestingRequest,
			stream: &receiveTestStream{
				reader: newInvalidReceiveMessageReader(),
			},
		}

		request, err := receiver.ingestRequst()

		require.Error(t, err)
		assert.Nil(t, request)
	})
}

func TestProcessResponse(t *testing.T) {
	t.Parallel()

	t.Run("accepts request by default", func(t *testing.T) {
		t.Parallel()

		receiver := &transferReceive{
			request: &models.TransferRequest{
				Filename: "hello.txt",
				FileSize: 5,
			},
		}

		response, err := receiver.processResponse()

		require.NoError(t, err)
		require.NotNil(t, response)

		assert.True(t, response.GetAccepted())
		assert.Empty(t, response.GetRejectReason())
	})
}
