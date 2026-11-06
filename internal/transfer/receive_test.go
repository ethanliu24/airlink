package transfer

import (
	"airlink/internal/event"
	"airlink/internal/transfer/models"
	"bytes"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

const receiveStateTestTimeout = 2 * time.Second

func newReceiveTestStream(
	readFunc func([]byte) (int, error),
	writeFunc func([]byte) (int, error),
) (*mockStream, <-chan struct{}, func() int) {
	if readFunc == nil {
		readFunc = func([]byte) (int, error) {
			return 0, io.EOF
		}
	}

	if writeFunc == nil {
		writeFunc = func(data []byte) (int, error) {
			return len(data), nil
		}
	}

	closed := make(chan struct{})

	var (
		closeOnce  sync.Once
		mu         sync.Mutex
		closeCount int
	)

	stream := &mockStream{
		readFunc:  readFunc,
		writeFunc: writeFunc,
		closeFunc: func() error {
			mu.Lock()
			closeCount++
			mu.Unlock()

			closeOnce.Do(func() {
				close(closed)
			})

			return nil
		},
	}

	getCloseCount := func() int {
		mu.Lock()
		defer mu.Unlock()

		return closeCount
	}

	return stream, closed, getCloseCount
}

func newReceiveTestInput(
	t *testing.T,
	request *models.TransferRequest,
	fileData []byte,
) *bytes.Reader {
	t.Helper()

	var incoming bytes.Buffer

	require.NoError(t, writeMessage(&incoming, request))

	_, err := incoming.Write(fileData)
	require.NoError(t, err)

	return bytes.NewReader(incoming.Bytes())
}

func newReceiveTestTransfer(stream io.ReadWriteCloser) *receive {
	return newReceiveTransfer(stream, func(event.Event) error {
		return nil
	})
}

func newValidReceiveRequest() *models.TransferRequest {
	return &models.TransferRequest{
		Id:        "request-123",
		Filename:  "hello.txt",
		Extension: ".txt",
		FileSize:  5,
		Mime:      "text/plain",
		SenderIp:  "192.0.2.10",
	}
}

func waitForReceiveChannel(
	t *testing.T,
	ch <-chan struct{},
	resource string,
) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(receiveStateTestTimeout):
		t.Fatalf("timed out waiting for %s", resource)
	}
}

func waitForReceiveResult(t *testing.T, done <-chan TransferResult) TransferResult {
	t.Helper()

	select {
	case result := <-done:
		return result
	case <-time.After(receiveStateTestTimeout):
		t.Fatal("timed out waiting for receive transfer")
		return nil
	}
}

func waitForReceiveEvent(t *testing.T, events <-chan event.Event) event.Event {
	t.Helper()

	select {
	case got := <-events:
		return got
	case <-time.After(receiveStateTestTimeout):
		t.Fatal("timed out waiting for transfer event")
		return nil
	}
}

func assertReceiveStreamClosedOnce(
	t *testing.T,
	closeCount func() int,
) {
	t.Helper()

	assert.Equal(t, 1, closeCount(), "stream should be closed exactly once")
}

func TestNewReceiveTransfer(t *testing.T) {
	t.Parallel()

	stream, _, _ := newReceiveTestStream(nil, nil)

	transfer := newReceiveTransfer(
		stream,
		func(event.Event) error {
			return nil
		},
	)

	require.NotNil(t, transfer)

	assert.Empty(t, transfer.id)
	assert.Equal(t, receiveInit, transfer.state)
	assert.NotNil(t, transfer.request)
	assert.Same(t, stream, transfer.stream)
	assert.NotNil(t, transfer.acceptTransfer)
	assert.False(t, transfer.consumed)
	assert.NotNil(t, transfer.emitEvent)
}

func TestReceiveSetID(t *testing.T) {
	t.Parallel()

	t.Run("sets ID", func(t *testing.T) {
		t.Parallel()

		transfer := newReceiveTestTransfer(nil)
		id := TransferId("transfer-123")

		transfer.setId(id)

		assert.Equal(t, id, transfer.getId())
	})

	t.Run("only sets ID once", func(t *testing.T) {
		t.Parallel()

		transfer := newReceiveTestTransfer(nil)

		transfer.setId("first-id")
		transfer.setId("second-id")

		assert.Equal(t, TransferId("first-id"), transfer.getId())
	})
}

func TestReceiveIngestRequest(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		expectedRequest := newValidReceiveRequest()

		incoming := newReceiveTestInput(t, expectedRequest, nil)

		stream, _, _ := newReceiveTestStream(incoming.Read, nil)

		transfer := newReceiveTestTransfer(stream)
		transfer.state = receiveIngestingRequest

		err := transfer.ingestRequst()

		require.NoError(t, err)
		assert.True(t, proto.Equal(expectedRequest, transfer.request))
	})

	t.Run("invalid state", func(t *testing.T) {
		t.Parallel()

		stream, _, _ := newReceiveTestStream(nil, nil)

		transfer := newReceiveTestTransfer(stream)
		transfer.state = receiveInit

		err := transfer.ingestRequst()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "receive state is not")
	})

	t.Run("nil request", func(t *testing.T) {
		t.Parallel()

		stream, _, _ := newReceiveTestStream(nil, nil)

		transfer := newReceiveTestTransfer(stream)
		transfer.state = receiveIngestingRequest
		transfer.request = nil

		err := transfer.ingestRequst()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "receive request nil")
	})

	t.Run("stream read error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream read failed")

		stream, _, _ := newReceiveTestStream(
			func([]byte) (int, error) {
				return 0, expectedErr
			},
			nil,
		)

		transfer := newReceiveTestTransfer(stream)
		transfer.state = receiveIngestingRequest

		err := transfer.ingestRequst()

		require.ErrorIs(t, err, expectedErr)
	})

	t.Run("malformed protobuf", func(t *testing.T) {
		t.Parallel()

		incoming := bytes.NewReader([]byte{
			0, 0, 0, 1, 0xff,
		})

		stream, _, _ := newReceiveTestStream(incoming.Read, nil)

		transfer := newReceiveTestTransfer(stream)
		transfer.state = receiveIngestingRequest

		err := transfer.ingestRequst()

		require.Error(t, err)
	})
}

func TestReceiveRequestUserTransferAcceptance(t *testing.T) {
	t.Parallel()

	t.Run("emits acceptance event", func(t *testing.T) {
		t.Parallel()

		var emitted event.Event

		transfer := newReceiveTestTransfer(nil)
		transfer.emitEvent = func(got event.Event) error {
			emitted = got
			return nil
		}

		id := TransferId("transfer-123")

		err := transfer.requestUserTransferAcceptance(id)

		require.NoError(t, err)
		require.NotNil(t, emitted)

		assert.Equal(t, RequestTransferAcceptanceEvtName, emitted.Name())

		payload, ok := emitted.Payload().(*RequestTransferAcceptancePayload)
		require.True(t, ok)

		assert.Equal(t, id, payload.transferId)
	})

	t.Run("propagates event publishing error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("event publishing failed")

		transfer := newReceiveTestTransfer(nil)
		transfer.emitEvent = func(event.Event) error {
			return expectedErr
		}

		err := transfer.requestUserTransferAcceptance("transfer-123")

		require.ErrorIs(t, err, expectedErr)
	})
}

func TestReceiveUserTransferAcceptanceDecision(t *testing.T) {
	t.Parallel()

	transfer := newReceiveTestTransfer(nil)

	decisionDone := make(chan struct{})

	go func() {
		transfer.userTransferAcceptanceDecision(true)
		close(decisionDone)
	}()

	// The channel is unbuffered, so the decision method blocks
	// until the receiving side consumes the value.
	select {
	case accepted := <-transfer.acceptTransfer:
		assert.True(t, accepted)

	case <-time.After(receiveStateTestTimeout):
		t.Fatal("timed out waiting for acceptance decision")
	}

	waitForReceiveChannel(t, decisionDone, "decision submission")

	// A second decision should be ignored because of sync.Once.
	assert.NotPanics(t, func() {
		transfer.userTransferAcceptanceDecision(false)
	})

	_, ok := <-transfer.acceptTransfer
	assert.False(t, ok, "acceptance channel should be closed")
}

func TestReceiveProcessResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		accepted bool
	}{
		{
			name:     "accepted",
			accepted: true,
		},
		{
			name:     "rejected",
			accepted: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			events := make(chan event.Event, 1)

			transfer := newReceiveTestTransfer(nil)
			transfer.id = "transfer-123"
			transfer.state = receiveProcessingRequest
			transfer.request = newValidReceiveRequest()
			transfer.emitEvent = func(got event.Event) error {
				events <- got
				return nil
			}

			type responseResult struct {
				response        *models.TransferResponse
				destinationPath string
				err             error
			}

			resultCh := make(chan responseResult, 1)

			go func() {
				response, path, err := transfer.processResponse()

				resultCh <- responseResult{
					response:        response,
					destinationPath: path,
					err:             err,
				}
			}()

			// Verify that the event is emitted before the transfer
			// waits for the user's decision.
			gotEvent := waitForReceiveEvent(t, events)

			assert.Equal(t, RequestTransferAcceptanceEvtName, gotEvent.Name())

			payload, ok := gotEvent.Payload().(*RequestTransferAcceptancePayload)
			require.True(t, ok)
			assert.Equal(t, transfer.id, payload.transferId)

			// Supply the user's decision.
			transfer.userTransferAcceptanceDecision(tt.accepted)

			var result responseResult

			select {
			case result = <-resultCh:
			case <-time.After(receiveStateTestTimeout):
				t.Fatal("timed out waiting for response processing")
			}

			require.NoError(t, result.err)
			require.NotNil(t, result.response)

			assert.Equal(t, tt.accepted, result.response.GetAccepted())
			assert.Equal(t, transfer.request.GetId(), result.response.GetId())

			assert.True(t, filepath.IsAbs(result.destinationPath))
			assert.Equal(
				t,
				transfer.request.GetFilename(),
				filepath.Base(result.destinationPath),
			)
		})
	}

	t.Run("invalid state", func(t *testing.T) {
		t.Parallel()

		transfer := newReceiveTestTransfer(nil)
		transfer.state = receiveInit

		response, path, err := transfer.processResponse()

		require.Error(t, err)
		assert.Nil(t, response)
		assert.Empty(t, path)
	})

	t.Run("event publishing error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("event publishing failed")

		transfer := newReceiveTestTransfer(nil)
		transfer.id = "transfer-123"
		transfer.state = receiveProcessingRequest
		transfer.request = newValidReceiveRequest()
		transfer.emitEvent = func(event.Event) error {
			return expectedErr
		}

		response, path, err := transfer.processResponse()

		require.ErrorIs(t, err, expectedErr)
		assert.Nil(t, response)
		assert.Empty(t, path)
	})
}

func TestReceiveResolveDestination(t *testing.T) {
	t.Parallel()

	transfer := newReceiveTestTransfer(nil)

	t.Run("resolves absolute destination", func(t *testing.T) {
		t.Parallel()

		path, err := transfer.resolveDestination("hello.txt")

		require.NoError(t, err)

		assert.True(t, filepath.IsAbs(path))
		assert.Equal(t, "hello.txt", filepath.Base(path))
	})

	t.Run("uses base filename", func(t *testing.T) {
		t.Parallel()

		path, err := transfer.resolveDestination("../../outside/hello.txt")

		require.NoError(t, err)

		assert.True(t, filepath.IsAbs(path))
		assert.Equal(t, "hello.txt", filepath.Base(path))
	})
}

func TestReceiveSendResponse(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		var outgoing bytes.Buffer

		stream, _, _ := newReceiveTestStream(nil, outgoing.Write)

		transfer := newReceiveTestTransfer(stream)
		transfer.state = receiveRespond
		transfer.response = &models.TransferResponse{
			Id:       "request-123",
			Accepted: true,
		}

		err := transfer.sendResponse()

		require.NoError(t, err)

		response := &models.TransferResponse{}
		require.NoError(t, readMessage(&outgoing, response))

		assert.Equal(t, "request-123", response.GetId())
		assert.True(t, response.GetAccepted())
	})

	t.Run("invalid state", func(t *testing.T) {
		t.Parallel()

		stream, _, _ := newReceiveTestStream(nil, nil)

		transfer := newReceiveTestTransfer(stream)
		transfer.state = receiveProcessingRequest
		transfer.response = &models.TransferResponse{
			Accepted: true,
		}

		err := transfer.sendResponse()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "receive state is not")
	})

	t.Run("write error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream write failed")

		stream, _, _ := newReceiveTestStream(
			nil,
			func([]byte) (int, error) {
				return 0, expectedErr
			},
		)

		transfer := newReceiveTestTransfer(stream)
		transfer.state = receiveRespond
		transfer.response = &models.TransferResponse{
			Accepted: true,
		}

		err := transfer.sendResponse()

		require.ErrorIs(t, err, expectedErr)
	})
}

func TestReceiveData(t *testing.T) {
	t.Parallel()

	t.Run("receives data", func(t *testing.T) {
		t.Parallel()

		data := "hello world"
		incoming := bytes.NewReader([]byte(data))

		stream, _, _ := newReceiveTestStream(incoming.Read, nil)

		transfer := newReceiveTestTransfer(stream)
		transfer.state = receiveReceiving

		n, err := transfer.receiveData()

		require.NoError(t, err)
		assert.Equal(t, len(data), n)
	})

	t.Run("invalid state", func(t *testing.T) {
		t.Parallel()

		stream, _, _ := newReceiveTestStream(nil, nil)

		transfer := newReceiveTestTransfer(stream)
		transfer.state = receiveInit

		n, err := transfer.receiveData()

		require.Error(t, err)
		assert.Zero(t, n)
		assert.Contains(t, err.Error(), "receive state is not")
	})

	t.Run("read error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream read failed")

		stream, _, _ := newReceiveTestStream(
			func([]byte) (int, error) {
				return 0, expectedErr
			},
			nil,
		)

		transfer := newReceiveTestTransfer(stream)
		transfer.state = receiveReceiving

		n, err := transfer.receiveData()

		require.ErrorIs(t, err, expectedErr)
		assert.Zero(t, n)
	})

	t.Run("counts bytes before read error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream read failed")
		readCalls := 0

		stream, _, _ := newReceiveTestStream(
			func(p []byte) (int, error) {
				readCalls++

				if readCalls == 1 {
					return copy(p, []byte("hello")), nil
				}

				return 0, expectedErr
			},
			nil,
		)

		transfer := newReceiveTestTransfer(stream)
		transfer.state = receiveReceiving

		n, err := transfer.receiveData()

		require.ErrorIs(t, err, expectedErr)
		assert.Equal(t, 5, n)
	})
}

func TestReceiveRun(t *testing.T) {
	t.Parallel()

	t.Run("successful receive", func(t *testing.T) {
		t.Parallel()

		const (
			transferID = TransferId("transfer-123")
			fileData   = "hello"
		)

		request := newValidReceiveRequest()

		incoming := newReceiveTestInput(
			t,
			request,
			[]byte(fileData),
		)

		var outgoing bytes.Buffer

		stream, closed, closeCount := newReceiveTestStream(
			incoming.Read,
			outgoing.Write,
		)

		emitted := make(chan event.Event, 1)

		transfer := newReceiveTransfer(
			stream,
			func(got event.Event) error {
				emitted <- got
				return nil
			},
		)
		transfer.setId(transferID)

		resultCh := make(chan TransferResult, 1)

		go func() {
			resultCh <- transfer.run()
		}()

		// Wait for the application acceptance event.
		gotEvent := waitForReceiveEvent(t, emitted)

		assert.Equal(t, RequestTransferAcceptanceEvtName, gotEvent.Name())

		payload, ok := gotEvent.Payload().(*RequestTransferAcceptancePayload)
		require.True(t, ok)
		assert.Equal(t, transferID, payload.transferId)

		// Approve the transfer.
		transfer.userTransferAcceptanceDecision(true)

		result := waitForReceiveResult(t, resultCh)

		waitForReceiveChannel(t, closed, "stream")
		assertReceiveStreamClosedOnce(t, closeCount)

		receiveResult, ok := result.(*receiveResult)
		require.True(t, ok)

		require.NoError(t, receiveResult.Err)
		assert.True(t, proto.Equal(request, receiveResult.Request))
		assert.True(t, receiveResult.Response.GetAccepted())
		assert.Equal(t, request.GetId(), receiveResult.Response.GetId())

		assert.True(t, filepath.IsAbs(receiveResult.DestinationPath))
		assert.Equal(
			t,
			request.GetFilename(),
			filepath.Base(receiveResult.DestinationPath),
		)

		assert.Equal(t, receiveDone, transfer.state)
		assert.True(t, transfer.consumed)

		// Verify the response was written to the stream.
		response := &models.TransferResponse{}

		err := readMessage(bytes.NewReader(outgoing.Bytes()), response)

		require.NoError(t, err)
		assert.True(t, response.GetAccepted())
		assert.Equal(t, request.GetId(), response.GetId())
	})

	t.Run("rejected receive", func(t *testing.T) {
		t.Parallel()

		request := newValidReceiveRequest()
		incoming := newReceiveTestInput(t, request, nil)

		var outgoing bytes.Buffer

		stream, closed, closeCount := newReceiveTestStream(
			incoming.Read,
			outgoing.Write,
		)

		emitted := make(chan event.Event, 1)

		transfer := newReceiveTransfer(
			stream,
			func(got event.Event) error {
				emitted <- got
				return nil
			},
		)
		transfer.setId("transfer-123")

		resultCh := make(chan TransferResult, 1)

		go func() {
			resultCh <- transfer.run()
		}()

		waitForReceiveEvent(t, emitted)

		// Reject the transfer.
		transfer.userTransferAcceptanceDecision(false)

		result := waitForReceiveResult(t, resultCh)

		waitForReceiveChannel(t, closed, "stream")
		assertReceiveStreamClosedOnce(t, closeCount)

		receiveResult, ok := result.(*receiveResult)
		require.True(t, ok)

		require.NoError(t, receiveResult.Err)
		assert.False(t, receiveResult.Response.GetAccepted())
		assert.Equal(t, receiveDone, transfer.state)

		response := &models.TransferResponse{}
		require.NoError(t, readMessage(bytes.NewReader(outgoing.Bytes()), response))

		assert.False(t, response.GetAccepted())
	})

	t.Run("missing transfer ID", func(t *testing.T) {
		t.Parallel()

		readCalled := false

		stream, closed, closeCount := newReceiveTestStream(
			func([]byte) (int, error) {
				readCalled = true
				return 0, io.EOF
			},
			nil,
		)

		transfer := newReceiveTestTransfer(stream)

		result := transfer.run()

		waitForReceiveChannel(t, closed, "stream")
		assertReceiveStreamClosedOnce(t, closeCount)

		receiveResult, ok := result.(*receiveResult)
		require.True(t, ok)

		require.Error(t, receiveResult.Err)
		assert.Contains(t, receiveResult.Err.Error(), "id not set")
		assert.False(t, readCalled)
		assert.Equal(t, receiveError, transfer.state)
		assert.True(t, transfer.consumed)
	})

	t.Run("event publishing error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("event publishing failed")

		request := newValidReceiveRequest()
		incoming := newReceiveTestInput(t, request, nil)

		stream, closed, closeCount := newReceiveTestStream(
			incoming.Read,
			nil,
		)

		transfer := newReceiveTransfer(
			stream,
			func(event.Event) error {
				return expectedErr
			},
		)
		transfer.setId("transfer-123")

		result := transfer.run()

		waitForReceiveChannel(t, closed, "stream")
		assertReceiveStreamClosedOnce(t, closeCount)

		receiveResult, ok := result.(*receiveResult)
		require.True(t, ok)

		require.ErrorIs(t, receiveResult.Err, expectedErr)
		assert.Equal(t, receiveError, transfer.state)
	})

	t.Run("request read error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream read failed")

		stream, closed, closeCount := newReceiveTestStream(
			func([]byte) (int, error) {
				return 0, expectedErr
			},
			nil,
		)

		transfer := newReceiveTestTransfer(stream)
		transfer.setId("transfer-123")

		result := transfer.run()

		waitForReceiveChannel(t, closed, "stream")
		assertReceiveStreamClosedOnce(t, closeCount)

		receiveResult, ok := result.(*receiveResult)
		require.True(t, ok)

		require.ErrorIs(t, receiveResult.Err, expectedErr)
		assert.Equal(t, receiveError, transfer.state)
	})

	t.Run("malformed request", func(t *testing.T) {
		t.Parallel()

		incoming := bytes.NewReader([]byte{
			0, 0, 0, 1, 0xff,
		})

		stream, closed, closeCount := newReceiveTestStream(
			incoming.Read,
			nil,
		)

		transfer := newReceiveTestTransfer(stream)
		transfer.setId("transfer-123")

		result := transfer.run()

		waitForReceiveChannel(t, closed, "stream")
		assertReceiveStreamClosedOnce(t, closeCount)

		receiveResult, ok := result.(*receiveResult)
		require.True(t, ok)

		require.Error(t, receiveResult.Err)
		assert.Equal(t, receiveError, transfer.state)
	})

	t.Run("response write error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("response write failed")

		request := newValidReceiveRequest()
		incoming := newReceiveTestInput(t, request, nil)

		stream, closed, closeCount := newReceiveTestStream(
			incoming.Read,
			func([]byte) (int, error) {
				return 0, expectedErr
			},
		)

		emitted := make(chan event.Event, 1)

		transfer := newReceiveTransfer(
			stream,
			func(got event.Event) error {
				emitted <- got
				return nil
			},
		)
		transfer.setId("transfer-123")

		resultCh := make(chan TransferResult, 1)

		go func() {
			resultCh <- transfer.run()
		}()

		waitForReceiveEvent(t, emitted)

		transfer.userTransferAcceptanceDecision(true)

		result := waitForReceiveResult(t, resultCh)

		waitForReceiveChannel(t, closed, "stream")
		assertReceiveStreamClosedOnce(t, closeCount)

		receiveResult, ok := result.(*receiveResult)
		require.True(t, ok)

		require.ErrorIs(t, receiveResult.Err, expectedErr)
		assert.Equal(t, receiveError, transfer.state)
	})
}

func TestUserTransferAcceptanceDecision(t *testing.T) {
	t.Parallel()

	t.Run("decision can be submitted before receiver waits", func(t *testing.T) {
		t.Parallel()

		transfer := &receive{
			acceptTransfer: make(chan bool, 1),
		}

		submitted := make(chan struct{})

		go func() {
			transfer.userTransferAcceptanceDecision(true)
			close(submitted)
		}()

		// Decision submission should not block.
		select {
		case <-submitted:
		case <-time.After(receiveStateTestTimeout):
			t.Fatal("decision submission blocked")
		}

		accepted, ok := <-transfer.acceptTransfer

		require.True(t, ok)
		assert.True(t, accepted)
	})

	t.Run("only first decision is accepted", func(t *testing.T) {
		t.Parallel()

		transfer := &receive{
			acceptTransfer: make(chan bool, 1),
		}

		transfer.userTransferAcceptanceDecision(false)
		transfer.userTransferAcceptanceDecision(true)

		accepted, ok := <-transfer.acceptTransfer

		require.True(t, ok)
		assert.False(t, accepted)

		// Channel should be closed after the first decision.
		_, ok = <-transfer.acceptTransfer
		assert.False(t, ok)
	})
}
