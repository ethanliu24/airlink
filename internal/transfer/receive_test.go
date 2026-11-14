package transfer

import (
	"airlink/internal/event"
	"airlink/internal/transfer/models"

	"bytes"
	"context"
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

var errReceiveTestStreamAborted = errors.New("receive test stream aborted")

func newReceiveTestStream(
	readFunc func([]byte) (int, error),
	writeFunc func([]byte) (int, error),
) (*mockStream, <-chan struct{}, func() int) {
	stream, closed, getCloseCount, _, _ := newReceiveTestStreamWithAbort(readFunc, writeFunc)
	return stream, closed, getCloseCount
}

func newReceiveTestStreamWithAbort(
	readFunc func([]byte) (int, error),
	writeFunc func([]byte) (int, error),
) (*mockStream, <-chan struct{}, func() int, <-chan struct{}, func() int) {
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
	aborted := make(chan struct{})

	var (
		closeOnce  sync.Once
		abortOnce  sync.Once
		mu         sync.Mutex
		closeCount int
		abortCount int
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
		abortFunc: func() {
			mu.Lock()
			abortCount++
			mu.Unlock()

			abortOnce.Do(func() {
				close(aborted)
			})
		},
	}

	getCloseCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return closeCount
	}

	getAbortCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return abortCount
	}

	return stream, closed, getCloseCount, aborted, getAbortCount
}

type receiveBlockingReader struct {
	input       *bytes.Reader
	readStarted chan struct{}
	releaseRead chan struct{}
	abortRead   <-chan struct{}
	startedOnce sync.Once
}

func (r *receiveBlockingReader) Read(p []byte) (int, error) {
	if r.input.Len() > 0 {
		return r.input.Read(p)
	}

	r.startedOnce.Do(func() {
		close(r.readStarted)
	})

	select {
	case <-r.releaseRead:
		return 0, io.EOF
	case <-r.abortRead:
		return 0, errReceiveTestStreamAborted
	}
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

func newReceiveTestTransfer(
	stream TransferStream,
	handlers ...event.EmitEventFunc,
) *receive {
	emitEvent := event.EmitEventFunc(func(event.Event) error {
		return nil
	})

	if len(handlers) > 0 {
		emitEvent = handlers[0]
	}

	return newReceiveTransfer(
		stream,
		emitEvent,
		context.Background(),
		func() {},
		receiveStateTestTimeout,
	)
}

func newReceiveTestTransferWithContext(
	stream TransferStream,
	emitEvent event.EmitEventFunc,
	ctx context.Context,
	cancel context.CancelFunc,
	timeout time.Duration,
) *receive {
	return newReceiveTransfer(stream, emitEvent, ctx, cancel, timeout)
}

func newValidReceiveRequest() *models.TransferRequest {
	return &models.TransferRequest{
		Id: "request-123",
		Filename: "hello.txt",
		Extension: ".txt",
		FileSize: 5,
		Mime: "text/plain",
		SenderIp: "192.0.2.10",
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

func waitForReceiveResult(
	t *testing.T,
	done <-chan TransferResult,
) TransferResult {
	t.Helper()

	select {
	case result := <-done:
		return result
	case <-time.After(receiveStateTestTimeout):
		t.Fatal("timed out waiting for receive transfer")
		return nil
	}
}

func waitForReceiveEvent(
	t *testing.T,
	events <-chan event.Event,
) event.Event {
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
	transfer := newReceiveTestTransfer(stream)

	require.NotNil(t, transfer)
	assert.Empty(t, transfer.id)
	assert.Equal(t, receiveInit, transfer.state)
	assert.NotNil(t, transfer.request)
	assert.Same(t, stream, transfer.stream)
	assert.NotNil(t, transfer.acceptTransfer)
	assert.Equal(t, 1, cap(transfer.acceptTransfer))
	assert.NotNil(t, transfer.selectedDestination)
	assert.Equal(t, 1, cap(transfer.selectedDestination))
	assert.False(t, transfer.consumed)
	assert.NotNil(t, transfer.emitEvent)
	assert.NotNil(t, transfer.ctx)
	assert.NotNil(t, transfer.cancel)
	assert.Equal(t, receiveStateTestTimeout, transfer.approvalWaitTimeoutDur)
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

		assert.Equal(t, id, payload.TransferId)
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

	t.Run("decision can be submitted before receiver waits", func(t *testing.T) {
		t.Parallel()

		transfer := newReceiveTestTransfer(nil)

		submitted := make(chan struct{})

		go func() {
			transfer.userTransferAcceptanceDecision(true)
			close(submitted)
		}()

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

		transfer := newReceiveTestTransfer(nil)

		transfer.userTransferAcceptanceDecision(false)
		transfer.userTransferAcceptanceDecision(true)

		accepted, ok := <-transfer.acceptTransfer

		require.True(t, ok)
		assert.False(t, accepted)

		_, ok = <-transfer.acceptTransfer
		assert.False(t, ok, "acceptance channel should be closed")
	})
}

func TestReceiveRequestDownloadDestination(t *testing.T) {
	t.Parallel()

	t.Run("emits destination request event", func(t *testing.T) {
		t.Parallel()

		var emitted event.Event

		transfer := newReceiveTestTransfer(nil)
		transfer.emitEvent = func(got event.Event) error {
			emitted = got
			return nil
		}

		id := TransferId("transfer-123")

		err := transfer.requestDownloadDestination(id)

		require.NoError(t, err)
		require.NotNil(t, emitted)

		assert.Equal(t, RequestDownloadDestinationEvtName, emitted.Name())

		payload, ok := emitted.Payload().(*RequestDownloadDestinationPayload)
		require.True(t, ok)

		assert.Equal(t, id, payload.TransferId)
	})

	t.Run("propagates event publishing error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("destination event publishing failed")

		transfer := newReceiveTestTransfer(nil)
		transfer.emitEvent = func(event.Event) error {
			return expectedErr
		}

		err := transfer.requestDownloadDestination("transfer-123")

		require.ErrorIs(t, err, expectedErr)
	})
}

func TestReceiveUserDownloadDestinationDecision(t *testing.T) {
	t.Parallel()

	t.Run("rejects empty path", func(t *testing.T) {
		t.Parallel()

		transfer := newReceiveTestTransfer(nil)

		err := transfer.userDownloadDestinationDecision("")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "destination path is empty")
	})

	t.Run("rejects relative path", func(t *testing.T) {
		t.Parallel()

		transfer := newReceiveTestTransfer(nil)

		err := transfer.userDownloadDestinationDecision("tmp/hello.txt")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "must be absolute")
	})

	t.Run("accepts absolute path and cleans it", func(t *testing.T) {
		t.Parallel()

		transfer := newReceiveTestTransfer(nil)

		dir := t.TempDir()

		path := dir +
			string(filepath.Separator) +
			"nested" +
			string(filepath.Separator) +
			".." +
			string(filepath.Separator) +
			"hello.txt"

		err := transfer.userDownloadDestinationDecision(path)

		require.NoError(t, err)

		got := <-transfer.selectedDestination

		assert.Equal(t, filepath.Clean(path), got)
		assert.True(t, filepath.IsAbs(got))
	})

	t.Run("decision can be submitted before receiver waits", func(t *testing.T) {
		t.Parallel()

		transfer := newReceiveTestTransfer(nil)
		path := filepath.Join(t.TempDir(), "hello.txt")

		submitted := make(chan error, 1)

		go func() {
			submitted <- transfer.userDownloadDestinationDecision(path)
		}()

		select {
		case err := <-submitted:
			require.NoError(t, err)
		case <-time.After(receiveStateTestTimeout):
			t.Fatal("destination submission blocked")
		}

		assert.Equal(t, filepath.Clean(path), <-transfer.selectedDestination)
	})

	t.Run("only first destination is accepted", func(t *testing.T) {
		t.Parallel()

		transfer := newReceiveTestTransfer(nil)

		firstPath := filepath.Join(t.TempDir(), "first.txt")
		secondPath := filepath.Join(t.TempDir(), "second.txt")

		require.NoError(t,
			transfer.userDownloadDestinationDecision(firstPath),
		)

		require.NoError(t,
			transfer.userDownloadDestinationDecision(secondPath),
		)

		got := <-transfer.selectedDestination

		assert.Equal(t, firstPath, got)

		_, ok := <-transfer.selectedDestination
		assert.False(t, ok)
	})
}

func TestReceiveProcessResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		accepted bool
	}{
		{
			name: "accepted",
			accepted: true,
		},
		{
			name: "rejected",
			accepted: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			events := make(chan event.Event, 2)

			transfer := newReceiveTestTransfer(nil)
			transfer.id = "transfer-123"
			transfer.state = receiveProcessingRequest
			transfer.request = newValidReceiveRequest()
			transfer.emitEvent = func(got event.Event) error {
				events <- got
				return nil
			}

			type responseResult struct {
				response *models.TransferResponse
				destinationPath string
				err error
			}

			resultCh := make(chan responseResult, 1)

			go func() {
				response, path, err := transfer.processResponse()

				resultCh <- responseResult{
					response: response,
					destinationPath: path,
					err: err,
				}
			}()

			// First event: request acceptance.
			gotEvent := waitForReceiveEvent(t, events)

			assert.Equal(t, RequestTransferAcceptanceEvtName, gotEvent.Name())

			payload, ok := gotEvent.Payload().(*RequestTransferAcceptancePayload)
			require.True(t, ok)
			assert.Equal(t, transfer.id, payload.TransferId)

			// Submit the user's acceptance decision.
			transfer.userTransferAcceptanceDecision(tt.accepted)

			var expectedPath string

			if tt.accepted {
				// Second event: request download destination.
				gotEvent = waitForReceiveEvent(t, events)

				assert.Equal(t, RequestDownloadDestinationEvtName, gotEvent.Name())

				destinationPayload, ok := gotEvent.Payload().(*RequestDownloadDestinationPayload)
				require.True(t, ok)
				assert.Equal(t, transfer.id, destinationPayload.TransferId)

				expectedPath = filepath.Join(t.TempDir(), "hello.txt")

				require.NoError(t,
					transfer.userDownloadDestinationDecision(expectedPath),
				)
			}

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
			assert.Equal(t, expectedPath, result.destinationPath)

			if !tt.accepted {
				assert.Empty(t, transfer.selectedDestination)
			}
		})
	}

	t.Run("times out and emits an event while waiting for acceptance", func(t *testing.T) {
		t.Parallel()

		events := make(chan event.Event, 1)
		stream, _, _, aborted, getAbortCount := newReceiveTestStreamWithAbort(nil, nil)
		transfer := newReceiveTestTransferWithContext(
			stream,
			func(got event.Event) error {
				events <- got
				return nil
			},
			context.Background(),
			func() {},
			30*time.Millisecond,
		)
		transfer.id = "transfer-123"
		transfer.state = receiveProcessingRequest
		transfer.request = newValidReceiveRequest()

		eventCh := make(chan event.Event, 1)
		originalEmitEvent := transfer.emitEvent
		transfer.emitEvent = func(e event.Event) error {
			for len(eventCh) > 0 {
				<-eventCh
			}

			eventCh <- e
			return originalEmitEvent(e)
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

		gotEvent := waitForReceiveEvent(t, events)
		assert.Equal(t, RequestTransferAcceptanceEvtName, gotEvent.Name())

		gotEvent = waitForReceiveEvent(t, events)
		assert.Equal(t, TransferTimedOutEvtName, gotEvent.Name())

		var result responseResult
		select {
		case result = <-resultCh:
		case <-time.After(receiveStateTestTimeout):
			t.Fatal("timed out waiting for approval deadline")
		}

		require.ErrorIs(t, result.err, context.DeadlineExceeded)
		assert.Nil(t, result.response)
		assert.Empty(t, result.destinationPath)
		waitForReceiveChannel(t, aborted, "stream abort")
		assert.Equal(t, 1, getAbortCount())
	})

	t.Run("times out and emits event while waiting for destination", func(t *testing.T) {
		t.Parallel()

		events := make(chan event.Event, 2)
		stream, _, _, aborted, getAbortCount := newReceiveTestStreamWithAbort(nil, nil)
		transfer := newReceiveTestTransferWithContext(
			stream,
			func(got event.Event) error {
				events <- got
				return nil
			},
			context.Background(),
			func() {},
			30*time.Millisecond,
		)
		transfer.id = "transfer-123"
		transfer.state = receiveProcessingRequest
		transfer.request = newValidReceiveRequest()

		eventCh := make(chan event.Event, 1)
		originalEmitEvent := transfer.emitEvent
		transfer.emitEvent = func(e event.Event) error {
			for len(eventCh) > 0 {
				<-eventCh
			}

			eventCh <- e
			return originalEmitEvent(e)
		}

		resultCh := make(chan error, 1)
		go func() {
			_, _, err := transfer.processResponse()
			resultCh <- err
		}()

		gotEvent := waitForReceiveEvent(t, events)
		assert.Equal(t, RequestTransferAcceptanceEvtName, gotEvent.Name())
		transfer.userTransferAcceptanceDecision(true)

		gotEvent = waitForReceiveEvent(t, events)
		assert.Equal(t, RequestDownloadDestinationEvtName, gotEvent.Name())

		gotEvent = waitForReceiveEvent(t, events)
		require.Equal(t, TransferTimedOutEvtName, gotEvent.Name())

		var err error
		select {
		case err = <-resultCh:
		case <-time.After(receiveStateTestTimeout):
			t.Fatal("timed out waiting for approval deadline")
		}

		require.ErrorIs(t, err, context.DeadlineExceeded)
		waitForReceiveChannel(t, aborted, "stream abort")
		assert.Equal(t, 1, getAbortCount())
	})

	t.Run("parent cancellation interrupts approval wait", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		events := make(chan event.Event, 1)
		stream, _, _, aborted, getAbortCount := newReceiveTestStreamWithAbort(nil, nil)
		transfer := newReceiveTestTransferWithContext(
			stream,
			func(got event.Event) error {
				events <- got
				return nil
			},
			ctx,
			cancel,
			time.Second,
		)
		transfer.id = "transfer-123"
		transfer.state = receiveProcessingRequest
		transfer.request = newValidReceiveRequest()

		resultCh := make(chan error, 1)
		go func() {
			_, _, err := transfer.processResponse()
			resultCh <- err
		}()

		gotEvent := waitForReceiveEvent(t, events)
		assert.Equal(t, RequestTransferAcceptanceEvtName, gotEvent.Name())
		cancel()

		select {
		case err := <-resultCh:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(receiveStateTestTimeout):
			t.Fatal("timed out waiting for parent cancellation")
		}

		waitForReceiveChannel(t, aborted, "stream abort")
		assert.Equal(t, 1, getAbortCount())
	})

	t.Run("parent cancellation interrupts destination wait", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		events := make(chan event.Event, 2)
		stream, _, _, aborted, getAbortCount := newReceiveTestStreamWithAbort(nil, nil)
		transfer := newReceiveTestTransferWithContext(
			stream,
			func(got event.Event) error {
				events <- got
				return nil
			},
			ctx,
			cancel,
			time.Second,
		)
		transfer.id = "transfer-123"
		transfer.state = receiveProcessingRequest
		transfer.request = newValidReceiveRequest()

		resultCh := make(chan error, 1)
		go func() {
			_, _, err := transfer.processResponse()
			resultCh <- err
		}()

		gotEvent := waitForReceiveEvent(t, events)
		assert.Equal(t, RequestTransferAcceptanceEvtName, gotEvent.Name())
		transfer.userTransferAcceptanceDecision(true)

		gotEvent = waitForReceiveEvent(t, events)
		assert.Equal(t, RequestDownloadDestinationEvtName, gotEvent.Name())
		cancel()

		select {
		case err := <-resultCh:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(receiveStateTestTimeout):
			t.Fatal("timed out waiting for parent cancellation")
		}

		waitForReceiveChannel(t, aborted, "stream abort")
		assert.Equal(t, 1, getAbortCount())
	})

	t.Run("invalid state", func(t *testing.T) {
		t.Parallel()

		transfer := newReceiveTestTransfer(nil)
		transfer.state = receiveInit

		response, path, err := transfer.processResponse()

		require.Error(t, err)
		assert.Nil(t, response)
		assert.Empty(t, path)
	})

	t.Run("acceptance event publishing error", func(t *testing.T) {

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

	t.Run("destination event publishing error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("destination event publishing failed")

		events := make(chan event.Event, 1)
		callCount := 0

		transfer := newReceiveTestTransfer(nil)
		transfer.id = "transfer-123"
		transfer.state = receiveProcessingRequest
		transfer.request = newValidReceiveRequest()
		transfer.emitEvent = func(e event.Event) error {
			callCount++

			if callCount == 2 {
				return expectedErr
			}

			events <- e
			return nil
		}

		resultCh := make(chan error, 1)

		go func() {
			_, _, err := transfer.processResponse()
			resultCh <- err
		}()

		gotEvent := waitForReceiveEvent(t, events)
		assert.Equal(t, RequestTransferAcceptanceEvtName, gotEvent.Name())

		transfer.userTransferAcceptanceDecision(true)

		select {
		case err := <-resultCh:
			require.ErrorIs(t, err, expectedErr)
		case <-time.After(receiveStateTestTimeout):
			t.Fatal("timed out waiting for destination event error")
		}
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
			Id: "request-123",
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
			fileData = "hello"
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

		emitted := make(chan event.Event, 2)

		transfer := newReceiveTestTransfer(stream, func(got event.Event) error { emitted <- got; return nil })
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
		assert.Equal(t, transferID, payload.TransferId)

		// Approve the transfer.
		transfer.userTransferAcceptanceDecision(true)

		// Wait for the destination selection event.
		destinationEvent := waitForReceiveEvent(t, emitted)

		assert.Equal(t, RequestDownloadDestinationEvtName, destinationEvent.Name())

		destinationPayload, ok := destinationEvent.Payload().(*RequestDownloadDestinationPayload)
		require.True(t, ok)
		assert.Equal(t, transferID, destinationPayload.TransferId)

		destinationPath := filepath.Join(t.TempDir(), "hello.txt")

		require.NoError(t,
			transfer.userDownloadDestinationDecision(destinationPath),
		)

		result := waitForReceiveResult(t, resultCh)

		waitForReceiveChannel(t, closed, "stream")
		assertReceiveStreamClosedOnce(t, closeCount)

		receiveResult, ok := result.(*receiveResult)
		require.True(t, ok)

		require.NoError(t, receiveResult.Err)
		assert.True(t, proto.Equal(request, receiveResult.Request))
		assert.True(t, receiveResult.Response.GetAccepted())
		assert.Equal(t, request.GetId(), receiveResult.Response.GetId())

		assert.Equal(t, destinationPath, receiveResult.DestinationPath)
		assert.True(t, filepath.IsAbs(receiveResult.DestinationPath))

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

		emitted := make(chan event.Event, 2)

		transfer := newReceiveTestTransfer(stream, func(got event.Event) error { emitted <- got; return nil })
		transfer.setId("transfer-123")

		resultCh := make(chan TransferResult, 1)

		go func() {
			resultCh <- transfer.run()
		}()

		// Wait for acceptance request.
		gotEvent := waitForReceiveEvent(t, emitted)
		assert.Equal(t, RequestTransferAcceptanceEvtName, gotEvent.Name())

		// Reject the transfer.
		transfer.userTransferAcceptanceDecision(false)

		result := waitForReceiveResult(t, resultCh)

		waitForReceiveChannel(t, closed, "stream")
		assertReceiveStreamClosedOnce(t, closeCount)

		receiveResult, ok := result.(*receiveResult)
		require.True(t, ok)

		require.NoError(t, receiveResult.Err)
		assert.False(t, receiveResult.Response.GetAccepted())
		assert.Empty(t, receiveResult.DestinationPath)
		assert.Equal(t, receiveDone, transfer.state)

		// No destination event should have been emitted.
		assert.Empty(t, emitted)

		response := &models.TransferResponse{}
		require.NoError(t, readMessage(bytes.NewReader(outgoing.Bytes()), response))

		assert.False(t, response.GetAccepted())
	})

	t.Run("approval timeout aborts and closes stream", func(t *testing.T) {
		t.Parallel()

		request := newValidReceiveRequest()
		incoming := newReceiveTestInput(t, request, nil)
		var outgoing bytes.Buffer
		stream, closed, closeCount, aborted, getAbortCount := newReceiveTestStreamWithAbort(
			incoming.Read,
			outgoing.Write,
		)

		emitted := make(chan event.Event, 1)
		transfer := newReceiveTestTransferWithContext(
			stream,
			func(got event.Event) error {
				emitted <- got
				return nil
			},
			context.Background(),
			func() {},
			30*time.Millisecond,
		)
		transfer.setId("transfer-123")

		resultCh := make(chan TransferResult, 1)
		go func() {
			resultCh <- transfer.run()
		}()

		gotEvent := waitForReceiveEvent(t, emitted)
		assert.Equal(t, RequestTransferAcceptanceEvtName, gotEvent.Name())

		result := waitForReceiveResult(t, resultCh)
		waitForReceiveChannel(t, aborted, "stream abort")
		waitForReceiveChannel(t, closed, "stream close")
		assertReceiveStreamClosedOnce(t, closeCount)

		receiveResult, ok := result.(*receiveResult)
		require.True(t, ok)
		require.ErrorIs(t, receiveResult.Err, context.DeadlineExceeded)
		assert.Equal(t, receiveError, transfer.state)
		assert.Equal(t, 1, getAbortCount())
	})

	t.Run("approval timeout does not apply while receiving file data", func(t *testing.T) {
		t.Parallel()

		approvalTimeout := 30 * time.Millisecond
		request := newValidReceiveRequest()
		incoming := newReceiveTestInput(t, request, nil)
		blockingReader := &receiveBlockingReader{
			input:       incoming,
			readStarted: make(chan struct{}),
			releaseRead: make(chan struct{}),
		}

		var outgoing bytes.Buffer
		stream, closed, closeCount, aborted, getAbortCount := newReceiveTestStreamWithAbort(
			blockingReader.Read,
			outgoing.Write,
		)
		blockingReader.abortRead = aborted

		emitted := make(chan event.Event, 2)
		transfer := newReceiveTestTransferWithContext(
			stream,
			func(got event.Event) error {
				emitted <- got
				return nil
			},
			context.Background(),
			func() {},
			approvalTimeout,
		)
		transfer.setId("transfer-123")

		resultCh := make(chan TransferResult, 1)
		go func() {
			resultCh <- transfer.run()
		}()

		gotEvent := waitForReceiveEvent(t, emitted)
		assert.Equal(t, RequestTransferAcceptanceEvtName, gotEvent.Name())
		transfer.userTransferAcceptanceDecision(true)

		gotEvent = waitForReceiveEvent(t, emitted)
		assert.Equal(t, RequestDownloadDestinationEvtName, gotEvent.Name())

		require.NoError(t, transfer.userDownloadDestinationDecision(
			filepath.Join(t.TempDir(), "hello.txt"),
		))

		waitForReceiveChannel(t, blockingReader.readStarted, "file data read")

		select {
		case result := <-resultCh:
			require.FailNow(t, "receive completed before blocked read was released", "result: %#v", result)
		case <-time.After(approvalTimeout * 3):
		}

		assert.Zero(t, getAbortCount())

		close(blockingReader.releaseRead)

		result := waitForReceiveResult(t, resultCh)
		waitForReceiveChannel(t, closed, "stream")
		assertReceiveStreamClosedOnce(t, closeCount)

		receiveResult, ok := result.(*receiveResult)
		require.True(t, ok)
		require.NoError(t, receiveResult.Err)
		assert.True(t, receiveResult.Response.GetAccepted())
		assert.Equal(t, receiveDone, transfer.state)
		assert.Zero(t, getAbortCount())
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

		transfer := newReceiveTestTransfer(stream, func(event.Event) error { return expectedErr })
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

		emitted := make(chan event.Event, 2)

		transfer := newReceiveTestTransfer(stream, func(got event.Event) error { emitted <- got; return nil })
		transfer.setId("transfer-123")

		resultCh := make(chan TransferResult, 1)

		go func() {
			resultCh <- transfer.run()
		}()

		// Acceptance request.
		waitForReceiveEvent(t, emitted)

		transfer.userTransferAcceptanceDecision(true)

		// Destination request.
		waitForReceiveEvent(t, emitted)

		require.NoError(t,
			transfer.userDownloadDestinationDecision(
				filepath.Join(t.TempDir(), "hello.txt"),
			),
		)

		result := waitForReceiveResult(t, resultCh)

		waitForReceiveChannel(t, closed, "stream")
		assertReceiveStreamClosedOnce(t, closeCount)

		receiveResult, ok := result.(*receiveResult)
		require.True(t, ok)

		require.ErrorIs(t, receiveResult.Err, expectedErr)
		assert.Equal(t, receiveError, transfer.state)
	})

	t.Run("parent cancellation interrupts file reception", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		request := newValidReceiveRequest()
		incoming := newReceiveTestInput(t, request, nil)

		blockingReader := &receiveBlockingReader{
			input:       incoming,
			readStarted: make(chan struct{}),
			releaseRead: make(chan struct{}),
		}

		var outgoing bytes.Buffer

		stream, closed, closeCount, aborted, getAbortCount :=
			newReceiveTestStreamWithAbort(
				blockingReader.Read,
				outgoing.Write,
			)

		blockingReader.abortRead = aborted

		emitted := make(chan event.Event, 2)

		transfer := newReceiveTestTransferWithContext(
			stream,
			func(got event.Event) error {
				emitted <- got
				return nil
			},
			ctx,
			cancel,
			time.Second,
		)
		transfer.setId("transfer-123")

		resultCh := make(chan TransferResult, 1)

		go func() {
			resultCh <- transfer.run()
		}()

		gotEvent := waitForReceiveEvent(t, emitted)
		assert.Equal(t, RequestTransferAcceptanceEvtName, gotEvent.Name())

		transfer.userTransferAcceptanceDecision(true)

		gotEvent = waitForReceiveEvent(t, emitted)
		assert.Equal(t, RequestDownloadDestinationEvtName, gotEvent.Name())

		destinationPath := filepath.Join(t.TempDir(), "hello.txt")

		require.NoError(t, transfer.userDownloadDestinationDecision(destinationPath))

		waitForReceiveChannel(t, blockingReader.readStarted, "file data read")

		cancel()

		result := waitForReceiveResult(t, resultCh)

		waitForReceiveChannel(t, aborted, "stream abort")
		waitForReceiveChannel(t, closed, "stream close")
		assertReceiveStreamClosedOnce(t, closeCount)

		receiveResult, ok := result.(*receiveResult)
		require.True(t, ok)

		require.ErrorIs(t, receiveResult.Err, context.Canceled)
		assert.True(t, receiveResult.Response.GetAccepted())
		assert.Equal(t, destinationPath, receiveResult.DestinationPath)
		assert.Equal(t, receiveError, transfer.state)
		assert.Equal(t, 1, getAbortCount())
	})
}
