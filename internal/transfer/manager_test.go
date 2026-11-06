package transfer

import (
	"airlink/internal/event"
	"airlink/internal/file"
	"airlink/internal/transfer/models"
	"bytes"
	"errors"
	"io"
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
            name:          "exhausted retries",
            existingIDs:   []string{"existing-id"},
            generatedIDs:  []string{
                "existing-id",
                "existing-id",
                "existing-id",
            },
            expectedCalls: idGenRetryTimes,
            expectedErr:   errors.New("manager receive transfer id generation max retries reached - impossible!"),
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

	for _, transferUnderTest := range []transfer {
		&send{},
		&receive{},
	}{
		t.Run("sets transfer id after registration", func(t *testing.T) {
			manager := &Manager{
				transfers: make(map[string]transfer),
				generateID: func () (TransferId, error) {
					return "test", nil
				},
			}

			id, err := manager.register(transferUnderTest)
			transferId := transferUnderTest.getId()

			require.NoError(t, err)
			require.NotNil(t, transferId)
			assert.Equal(t, id, "test")
			assert.Equal(t, transferId, "test")
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

		// Verify transfer registration.
		manager.mu.Lock()
		registeredTransfer, exists := manager.transfers[id]
		manager.mu.Unlock()

		require.True(t, exists)
		_, isSend := registeredTransfer.(*send)
		assert.True(t, isSend)

		// Wait for asynchronous transfer completion.
		waitForManagerChannel(t, readerClosed, "reader")
		waitForManagerChannel(t, streamClosed, "stream")

		assert.EqualValues(t, 1, readerCloseCount.Load())
		assert.EqualValues(t, 1, streamCloseCount.Load())

		// Verify request and file data.
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
			readOnce       sync.Once
			closeOnce      sync.Once
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
			generateID: func() (string, error) {
				return id, nil
			},
		}

		gotID, err := manager.Receive(stream)

		require.NoError(t, err)
		assert.Equal(t, TransferId(id), gotID)

		// Verify registration.
		manager.mu.Lock()
		registeredTransfer, exists := manager.transfers[id]
		manager.mu.Unlock()

		require.True(t, exists)

		_, isReceive := registeredTransfer.(*receive)
		assert.True(t, isReceive)

		// Verify that the transfer started.
		waitForManagerChannel(t, readStarted, "transfer read")

		// The mock read fails, so the transfer should terminate
		// and close its stream.
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
	mgr := NewManager(mockAddr, newMockReader, func(e event.Event) error { return nil })

	require.NotNil(t, mgr)
	assert.NotNil(t, mgr.transfers)
	assert.Empty(t, mgr.transfers)
	assert.Equal(t, mockAddr, mgr.addr)
	assert.NotNil(t, mgr.openReader)
	assert.NotNil(t, mgr.generateID)
}

func TestGetTransfer(t *testing.T) {
	t.Parallel()

	t.Run("transfer exists", func(t *testing.T) {
		id := "test"
		transfers := make(map[string]transfer)

		transfers[id] = &send{id: id}

		manager := &Manager{transfers: transfers}
		transfer, ok := manager.GetTransfer(id)

		require.True(t, ok)
		require.Equal(t, transfer.getId(), id)
	})

	t.Run("transfer does not exist", func(t *testing.T) {
		id := "test"
		transfers := make(map[string]transfer)

		transfers[id] = &send{id: id}

		manager := &Manager{transfers: transfers}
		transfer, ok := manager.GetTransfer("doesn't exist")

		require.False(t, ok)
		require.Nil(t, nil, transfer)
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

		// Verify the channel was closed after the decision.
		_, ok = <-receiveTransfer.acceptTransfer
		assert.False(t, ok)
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
