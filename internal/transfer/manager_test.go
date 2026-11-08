package transfer

import (
	"airlink/internal/event"
	"airlink/internal/file"
	"airlink/internal/transfer/models"
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func waitForManagerChannel(
	t *testing.T,
	ch <-chan struct{},
	resource string,
) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s to close", resource)
	}
}

func TestManagerRegister(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		existingIDs   []string
		generatedIDs  []string
		generateErr   error
		expectedID    string
		expectedCalls int
		expectedErr   error
	}{
		{
			name:          "successful registration",
			generatedIDs:  []string{"new-id"},
			expectedID:    "new-id",
			expectedCalls: 1,
		},
		{
			name:          "retry after ID collision",
			existingIDs:   []string{"existing-id"},
			generatedIDs:  []string{"existing-id", "new-id"},
			expectedID:    "new-id",
			expectedCalls: 2,
		},
		{
			name:          "ID generation error",
			generateErr:   errors.New("ID generation failed"),
			expectedCalls: 1,
			expectedErr:   errors.New("ID generation failed"),
		},
		{
			name:        "exhausted retries",
			existingIDs: []string{"existing-id"},
			generatedIDs: []string{
				"existing-id",
				"existing-id",
				"existing-id",
			},
			expectedCalls: idGenRetryTimes,
			expectedErr: errors.New(
				"manager receive transfer id generation max retries reached - impossible!",
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var transferUnderTest = &send{}

			existing := make(map[string]transfer)

			for _, id := range tt.existingIDs {
				existing[id] = nil
			}

			calls := 0

			manager := &Manager{
				transfers: existing,
				generateID: func() (string, error) {
					calls++

					if tt.generateErr != nil {
						return "", tt.generateErr
					}

					if calls > len(tt.generatedIDs) {
						return "", errors.New("unexpected ID generation call")
					}

					return tt.generatedIDs[calls-1], nil
				},
			}

			id, err := manager.register(transferUnderTest)

			assert.Equal(t, tt.expectedCalls, calls)

			if tt.expectedErr != nil {
				require.Error(t, err)
				assert.Empty(t, id)

				if tt.name == "ID generation error" {
					assert.ErrorIs(t, err, tt.generateErr)
				} else {
					assert.Contains(t, err.Error(), tt.expectedErr.Error())
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedID, id)

			manager.mu.Lock()
			registeredTransfer, exists := manager.transfers[id]
			manager.mu.Unlock()

			assert.True(t, exists)
			assert.Equal(t, transferUnderTest, registeredTransfer)
		})
	}

	for _, transferUnderTest := range []transfer{
		&send{},
		&receive{},
	} {
		t.Run("sets transfer id after registration", func(t *testing.T) {
			manager := &Manager{
				transfers: make(map[string]transfer),
				generateID: func() (string, error) {
					return "test", nil
				},
			}

			id, err := manager.register(transferUnderTest)
			transferId := transferUnderTest.getId()

			require.NoError(t, err)
			require.NotNil(t, transferId)

			assert.Equal(t, "test", id)
			assert.Equal(t, "test", transferId)
		})
	}
}

func TestManagerSend(t *testing.T) {
	t.Parallel()

	t.Run("successful send", func(t *testing.T) {
		t.Parallel()

		const (
			filename = "test.txt"
			data     = "hello world"
			id       = "transfer-123"
		)

		reader, err := newMockReader(data)
		require.NoError(t, err)

		mockFile := reader.(*mockReader)

		readerClosed := make(chan struct{})
		var readerCloseCount atomic.Int32
		var readerCloseOnce sync.Once

		mockFile.closeFunc = func() error {
			if readerCloseCount.Add(1) == 1 {
				readerCloseOnce.Do(func() {
					close(readerClosed)
				})
			}
			return nil
		}

		var response bytes.Buffer
		require.NoError(t, writeMessage(&response, &models.TransferResponse{
			Accepted: true,
		}))

		var written bytes.Buffer

		streamClosed := make(chan struct{})
		var streamCloseCount atomic.Int32
		var streamCloseOnce sync.Once

		stream := &mockStream{
			readFunc:  bytes.NewReader(response.Bytes()).Read,
			writeFunc: written.Write,
			closeFunc: func() error {
				if streamCloseCount.Add(1) == 1 {
					streamCloseOnce.Do(func() {
						close(streamClosed)
					})
				}
				return nil
			},
		}

		manager := &Manager{
			transfers: make(map[string]transfer),
			ctx:       context.Background(),
			addr:      testAddr(),
			openReader: func(gotFilename string) (file.Reader, error) {
				assert.Equal(t, filename, gotFilename)
				return reader, nil
			},
			generateID: func() (string, error) {
				return id, nil
			},
		}

		gotID, err := manager.Send(stream, filename)

		require.NoError(t, err)
		assert.Equal(t, id, string(gotID))

		manager.mu.Lock()
		registeredTransfer, exists := manager.transfers[id]
		manager.mu.Unlock()

		require.True(t, exists)

		_, isSend := registeredTransfer.(*send)
		assert.True(t, isSend)

		waitForManagerChannel(t, readerClosed, "reader")
		waitForManagerChannel(t, streamClosed, "stream")

		assert.EqualValues(t, 1, readerCloseCount.Load())
		assert.EqualValues(t, 1, streamCloseCount.Load())

		out := bytes.NewReader(written.Bytes())
		request := &models.TransferRequest{}

		require.NoError(t, readMessage(out, request))

		assert.Equal(t, "report.txt", request.GetFilename())
		assert.Equal(t, uint64(len(data)), request.GetFileSize())
		assert.NotEmpty(t, request.GetMime())

		fileData, err := io.ReadAll(out)
		require.NoError(t, err)
		assert.Equal(t, data, string(fileData))
	})

	t.Run("reader opening error closes stream", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("reader failed")

		streamClosed := make(chan struct{})
		var streamCloseCount atomic.Int32
		var streamCloseOnce sync.Once

		stream := &mockStream{
			closeFunc: func() error {
				if streamCloseCount.Add(1) == 1 {
					streamCloseOnce.Do(func() {
						close(streamClosed)
					})
				}
				return nil
			},
		}

		var generateIDCalls atomic.Int32

		manager := &Manager{
			transfers: make(map[string]transfer),
			ctx:       context.Background(),
			openReader: func(string) (file.Reader, error) {
				return nil, expectedErr
			},
			generateID: func() (string, error) {
				generateIDCalls.Add(1)
				return "unexpected-id", nil
			},
		}

		id, err := manager.Send(stream, "test.txt")

		require.ErrorIs(t, err, expectedErr)
		assert.Empty(t, id)
		assert.Zero(t, generateIDCalls.Load())

		waitForManagerChannel(t, streamClosed, "stream")
		assert.EqualValues(t, 1, streamCloseCount.Load())

		manager.mu.Lock()
		assert.Empty(t, manager.transfers)
		manager.mu.Unlock()
	})

	t.Run("registration error closes reader and stream", func(t *testing.T) {
		t.Parallel()

		generateErr := errors.New("ID generation failed")

		reader, err := newMockReader("hello world")
		require.NoError(t, err)

		mockFile := reader.(*mockReader)

		readerClosed := make(chan struct{})
		var readerCloseCount atomic.Int32
		var readerCloseOnce sync.Once

		mockFile.closeFunc = func() error {
			if readerCloseCount.Add(1) == 1 {
				readerCloseOnce.Do(func() {
					close(readerClosed)
				})
			}
			return nil
		}

		streamClosed := make(chan struct{})
		var streamCloseCount atomic.Int32
		var streamCloseOnce sync.Once

		stream := &mockStream{
			closeFunc: func() error {
				if streamCloseCount.Add(1) == 1 {
					streamCloseOnce.Do(func() {
						close(streamClosed)
					})
				}
				return nil
			},
		}

		manager := &Manager{
			transfers: make(map[string]transfer),
			ctx:       context.Background(),
			openReader: func(string) (file.Reader, error) {
				return reader, nil
			},
			generateID: func() (string, error) {
				return "", generateErr
			},
		}

		id, err := manager.Send(stream, "test.txt")

		require.ErrorIs(t, err, generateErr)
		assert.Empty(t, id)

		waitForManagerChannel(t, readerClosed, "reader")
		waitForManagerChannel(t, streamClosed, "stream")

		assert.EqualValues(t, 1, readerCloseCount.Load())
		assert.EqualValues(t, 1, streamCloseCount.Load())

		manager.mu.Lock()
		assert.Empty(t, manager.transfers)
		manager.mu.Unlock()
	})
}

func TestManagerReceive(t *testing.T) {
	t.Parallel()

	t.Run("successful registration starts transfer", func(t *testing.T) {
		t.Parallel()

		const id = "transfer-123"

		readStarted := make(chan struct{})
		streamClosed := make(chan struct{})

		var (
			readOnce         sync.Once
			closeOnce        sync.Once
			streamCloseCount atomic.Int32
		)

		stream := &mockStream{
			readFunc: func([]byte) (int, error) {
				readOnce.Do(func() {
					close(readStarted)
				})
				return 0, errors.New("mock stream read failed")
			},
			writeFunc: func(data []byte) (int, error) {
				return len(data), nil
			},
			closeFunc: func() error {
				streamCloseCount.Add(1)
				closeOnce.Do(func() {
					close(streamClosed)
				})
				return nil
			},
		}

		manager := &Manager{
			transfers: make(map[string]transfer),
			ctx:       context.Background(),
			generateID: func() (string, error) {
				return id, nil
			},
		}

		gotID, err := manager.Receive(stream)

		require.NoError(t, err)
		assert.Equal(t, TransferId(id), gotID)

		manager.mu.Lock()
		registeredTransfer, exists := manager.transfers[id]
		manager.mu.Unlock()

		require.True(t, exists)

		_, isReceive := registeredTransfer.(*receive)
		assert.True(t, isReceive)

		waitForManagerChannel(t, readStarted, "transfer read")
		waitForManagerChannel(t, streamClosed, "stream")

		assert.EqualValues(t, 1, streamCloseCount.Load())
	})

	t.Run("registration error closes stream", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("ID generation failed")

		streamClosed := make(chan struct{})

		var (
			streamCloseCount atomic.Int32
			closeOnce        sync.Once
			readCalled       atomic.Bool
		)

		stream := &mockStream{
			readFunc: func([]byte) (int, error) {
				readCalled.Store(true)
				return 0, io.EOF
			},
			writeFunc: func(data []byte) (int, error) {
				return len(data), nil
			},
			closeFunc: func() error {
				streamCloseCount.Add(1)
				closeOnce.Do(func() {
					close(streamClosed)
				})
				return nil
			},
		}

		manager := &Manager{
			transfers: make(map[string]transfer),
			ctx:       context.Background(),
			generateID: func() (string, error) {
				return "", expectedErr
			},
		}

		id, err := manager.Receive(stream)

		require.ErrorIs(t, err, expectedErr)
		assert.Empty(t, id)

		waitForManagerChannel(t, streamClosed, "stream")

		assert.EqualValues(t, 1, streamCloseCount.Load())
		assert.False(t, readCalled.Load(), "transfer should not have started")

		manager.mu.Lock()
		assert.Empty(t, manager.transfers)
		manager.mu.Unlock()
	})
}

func TestNewManager(t *testing.T) {
	mockAddr := testAddr()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := NewManager(
		mockAddr,
		newMockReader,
		func(e event.Event) error {
			return nil
		},
		ctx,
		cancel,
	)

	require.NotNil(t, mgr)
	assert.NotNil(t, mgr.transfers)
	assert.Empty(t, mgr.transfers)
	assert.Equal(t, mockAddr, mgr.addr)
	assert.Same(t, ctx, mgr.ctx)
	assert.NotNil(t, mgr.cancel)
	assert.NotNil(t, mgr.openReader)
	assert.NotNil(t, mgr.emitEvent)
	assert.NotNil(t, mgr.generateID)
}

func TestManagerCleanUp(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	manager := NewManager(
		testAddr(),
		newMockReader,
		func(event.Event) error { return nil },
		ctx,
		cancel,
	)

	manager.CleanUp()

	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("manager cleanup did not cancel its context")
	}

	assert.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestGetTransfer(t *testing.T) {
	t.Parallel()

	t.Run("transfer exists", func(t *testing.T) {
		id := "test"
		transfers := make(map[string]transfer)

		transfers[id] = &send{id: id}

		manager := &Manager{transfers: transfers}
		got, ok := manager.getTransfer(id)

		require.True(t, ok)
		require.Equal(t, id, got.getId())
	})

	t.Run("transfer does not exist", func(t *testing.T) {
		id := "test"
		transfers := make(map[string]transfer)

		transfers[id] = &send{id: id}

		manager := &Manager{transfers: transfers}
		got, ok := manager.getTransfer("doesn't exist")

		require.False(t, ok)
		require.Nil(t, got)
	})
}

func TestSubmitTransferAcceptDecision(t *testing.T) {
	t.Parallel()

	const id = TransferId("transfer-123")

	t.Run("submits acceptance decision", func(t *testing.T) {
		t.Parallel()

		receiveTransfer := &receive{
			acceptTransfer: make(chan bool, 1),
		}

		manager := &Manager{
			transfers: map[string]transfer{
				string(id): receiveTransfer,
			},
		}

		err := manager.SubmitTransferAcceptDecision(id, true)

		require.NoError(t, err)

		accepted, ok := <-receiveTransfer.acceptTransfer

		require.True(t, ok)
		assert.True(t, accepted)

		_, ok = <-receiveTransfer.acceptTransfer
		assert.False(t, ok, "acceptance channel should be closed")
	})

	t.Run("submits rejection decision", func(t *testing.T) {
		t.Parallel()

		receiveTransfer := &receive{
			acceptTransfer: make(chan bool, 1),
		}

		manager := &Manager{
			transfers: map[string]transfer{
				string(id): receiveTransfer,
			},
		}

		err := manager.SubmitTransferAcceptDecision(id, false)

		require.NoError(t, err)

		accepted, ok := <-receiveTransfer.acceptTransfer

		require.True(t, ok)
		assert.False(t, accepted)
	})

	t.Run("transfer does not exist", func(t *testing.T) {
		t.Parallel()

		manager := &Manager{
			transfers: make(map[string]transfer),
		}

		err := manager.SubmitTransferAcceptDecision(id, true)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "receive transfer does not exist")
	})

	t.Run("transfer is not a receive transfer", func(t *testing.T) {
		t.Parallel()

		manager := &Manager{
			transfers: map[string]transfer{
				string(id): &send{},
			},
		}

		err := manager.SubmitTransferAcceptDecision(id, true)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "transfer is not a receive transfer")
	})

	t.Run("only first decision is submitted", func(t *testing.T) {
		t.Parallel()

		receiveTransfer := &receive{
			acceptTransfer: make(chan bool, 1),
		}

		manager := &Manager{
			transfers: map[string]transfer{
				string(id): receiveTransfer,
			},
		}

		require.NoError(t, manager.SubmitTransferAcceptDecision(id, false))
		require.NoError(t, manager.SubmitTransferAcceptDecision(id, true))

		accepted, ok := <-receiveTransfer.acceptTransfer

		require.True(t, ok)
		assert.False(t, accepted)
	})
}

func TestSubmitDownloadDestination(t *testing.T) {
	t.Parallel()

	const id = TransferId("transfer-123")

	t.Run("submits absolute destination", func(t *testing.T) {
		t.Parallel()

		receiveTransfer := &receive{
			selectedDestination: make(chan string, 1),
		}

		manager := &Manager{
			transfers: map[string]transfer{
				string(id): receiveTransfer,
			},
		}

		path := filepath.Join(t.TempDir(), "hello.txt")

		err := manager.SubmitDownloadDestination(id, path)

		require.NoError(t, err)

		got := <-receiveTransfer.selectedDestination

		assert.Equal(t, filepath.Clean(path), got)
		assert.True(t, filepath.IsAbs(got))
	})

	t.Run("rejects empty destination", func(t *testing.T) {
		t.Parallel()

		receiveTransfer := &receive{
			selectedDestination: make(chan string, 1),
		}

		manager := &Manager{
			transfers: map[string]transfer{
				string(id): receiveTransfer,
			},
		}

		err := manager.SubmitDownloadDestination(id, "")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "destination path is empty")
		assert.Empty(t, receiveTransfer.selectedDestination)
	})

	t.Run("rejects relative destination", func(t *testing.T) {
		t.Parallel()

		receiveTransfer := &receive{
			selectedDestination: make(chan string, 1),
		}

		manager := &Manager{
			transfers: map[string]transfer{
				string(id): receiveTransfer,
			},
		}

		err := manager.SubmitDownloadDestination(id, "tmp/hello.txt")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "must be absolute")
		assert.Empty(t, receiveTransfer.selectedDestination)
	})

	t.Run("transfer does not exist", func(t *testing.T) {
		t.Parallel()

		manager := &Manager{
			transfers: make(map[string]transfer),
		}

		err := manager.SubmitDownloadDestination(id, filepath.Join(t.TempDir(), "hello.txt"))

		require.Error(t, err)
		assert.Contains(t, err.Error(), "receive transfer does not exist")
	})

	t.Run("transfer is not a receive transfer", func(t *testing.T) {
		t.Parallel()

		manager := &Manager{
			transfers: map[string]transfer{
				string(id): &send{},
			},
		}

		err := manager.SubmitDownloadDestination(
			id,
			filepath.Join(t.TempDir(), "hello.txt"),
		)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "transfer is not a receive transfer")
	})

	t.Run("only first destination is submitted", func(t *testing.T) {
		t.Parallel()

		receiveTransfer := &receive{
			selectedDestination: make(chan string, 1),
		}

		manager := &Manager{
			transfers: map[string]transfer{
				string(id): receiveTransfer,
			},
		}

		firstPath := filepath.Join(t.TempDir(), "first.txt")
		secondPath := filepath.Join(t.TempDir(), "second.txt")

		require.NoError(t, manager.SubmitDownloadDestination(id, firstPath))
		require.NoError(t, manager.SubmitDownloadDestination(id, secondPath))

		got := <-receiveTransfer.selectedDestination

		assert.Equal(t, firstPath, got)

		_, ok := <-receiveTransfer.selectedDestination
		assert.False(t, ok)
	})
}
