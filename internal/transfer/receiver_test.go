package transfer

import (
	"airlink/internal/transfer/models"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const receiverTestTimeout = 2 * time.Second

func newIncomingTransferStream(
	t *testing.T,
	request *models.TransferRequest,
	fileData []byte,
) (*mockStream, *bytes.Buffer, <-chan struct{}) {
	t.Helper()

	var incoming bytes.Buffer
	require.NoError(t, writeMessage(&incoming, request))

	_, err := incoming.Write(fileData)
	require.NoError(t, err)

	var outgoing bytes.Buffer
	closed := make(chan struct{})

	stream := &mockStream{
		readFunc:  incoming.Read,
		writeFunc: outgoing.Write,
		closeFunc: func() error {
			close(closed)
			return nil
		},
	}

	return stream, &outgoing, closed
}

func TestNewReceiver(t *testing.T) {
	t.Parallel()

	t.Run("success initialization", func(t *testing.T) {
		t.Parallel()

		transport := &quic.Transport{}

		receiver := NewReceiver(transport)

		require.NotNil(t, receiver)
		require.NotNil(t, receiver.listen)
		assert.Nil(t, receiver.listener)
	})
}

func TestHandleStream(t *testing.T) {
	t.Parallel()

	t.Run("processes request and closes stream", func(t *testing.T) {
		t.Parallel()

		request := &models.TransferRequest{
			Filename:  "hello.txt",
			Extension: ".txt",
			FileSize:  5,
			Mime:      "text/plain",
			SenderIp:  "192.0.2.10",
		}

		stream, outgoing, closed := newIncomingTransferStream(
			t,
			request,
			[]byte("hello"),
		)

		handleStream(stream)

		select {
		case <-closed:
		default:
			t.Fatal("stream was not closed")
		}

		response := &models.TransferResponse{}
		err := readMessage(bytes.NewReader(outgoing.Bytes()), response)

		require.NoError(t, err)
		assert.True(t, response.GetAccepted())
		assert.Empty(t, response.GetRejectReason())
	})

	t.Run("closes stream when request read fails", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("stream read failed")
		closed := make(chan struct{})

		stream := &mockStream{
			readFunc: func([]byte) (int, error) {
				return 0, expectedErr
			},
			writeFunc: func([]byte) (int, error) {
				return 0, errors.New("unexpected write")
			},
			closeFunc: func() error {
				close(closed)
				return nil
			},
		}

		handleStream(stream)

		select {
		case <-closed:
		default:
			t.Fatal("stream was not closed after read failure")
		}
	})

	t.Run("closes stream when request protobuf is malformed", func(t *testing.T) {
		t.Parallel()

		// A length-prefixed protobuf payload containing invalid wire data.
		incoming := bytes.NewReader([]byte{0, 0, 0, 1, 0xff})
		closed := make(chan struct{})

		stream := &mockStream{
			readFunc:  incoming.Read,
			writeFunc: func([]byte) (int, error) { return 0, errors.New("unexpected write") },
			closeFunc: func() error {
				close(closed)
				return nil
			},
		}

		handleStream(stream)

		select {
		case <-closed:
		default:
			t.Fatal("stream was not closed after malformed request")
		}
	})
}

func TestHandleConnection(t *testing.T) {
	t.Parallel()

	t.Run("removes connection after stream accept error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("connection closed")

		conn := &mockConn{
			acceptStreamFunc: func(context.Context) (Stream, error) {
				return nil, expectedErr
			},
			closeWithErrFunc: func(
				quic.ApplicationErrorCode,
				string,
			) error {
				return nil
			},
		}

		receiver := &Receiver{
			conns: map[Conn]struct{}{
				conn: {},
			},
		}

		receiver.handleConnection(conn)

		assert.NotContains(t, receiver.conns, conn)
	})

	t.Run("closes connection normally", func(t *testing.T) {
		t.Parallel()

		closeCalled := false

		conn := &mockConn{
			acceptStreamFunc: func(context.Context) (Stream, error) {
				return nil, errors.New("connection closed")
			},
			closeWithErrFunc: func(
				code quic.ApplicationErrorCode,
				msg string,
			) error {
				closeCalled = true

				assert.Equal(t, quic.ApplicationErrorCode(0), code)
				assert.Equal(t, "receiver connection closed normally", msg)

				return nil
			},
		}

		receiver := &Receiver{
			conns: map[Conn]struct{}{
				conn: {},
			},
		}

		receiver.handleConnection(conn)

		assert.True(t, closeCalled)
		assert.NotContains(t, receiver.conns, conn)
	})

	t.Run("handles accepted stream", func(t *testing.T) {
		t.Parallel()

		request := &models.TransferRequest{
			Filename: "hello.txt",
			FileSize: 5,
			Mime:     "text/plain",
		}

		stream, outgoing, streamClosed := newIncomingTransferStream(
			t,
			request,
			[]byte("hello"),
		)

		acceptCalls := 0

		conn := &mockConn{
			acceptStreamFunc: func(context.Context) (Stream, error) {
				acceptCalls++

				if acceptCalls == 1 {
					return stream, nil
				}

				return nil, errors.New("connection closed")
			},
			closeWithErrFunc: func(
				quic.ApplicationErrorCode,
				string,
			) error {
				return nil
			},
		}

		receiver := &Receiver{
			conns: map[Conn]struct{}{
				conn: {},
			},
		}

		receiver.handleConnection(conn)

		select {
		case <-streamClosed:
		case <-time.After(receiverTestTimeout):
			t.Fatal("timed out waiting for accepted stream to finish")
		}

		assert.Equal(t, 2, acceptCalls)
		assert.NotContains(t, receiver.conns, conn)

		response := &models.TransferResponse{}
		err := readMessage(bytes.NewReader(outgoing.Bytes()), response)

		require.NoError(t, err)
		assert.True(t, response.GetAccepted())
	})
}

func TestReceiverListen(t *testing.T) {
	t.Parallel()

	t.Run("listen error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("listen failed")

		receiver := &Receiver{
			listen: func(*tls.Config, *quic.Config) (Listener, error) {
				return nil, expectedErr
			},
		}

		err := receiver.Listen(&tls.Config{}, &quic.Config{})

		require.ErrorIs(t, err, expectedErr)
		assert.Nil(t, receiver.listener)
	})

	t.Run("success starts listener accept loop", func(t *testing.T) {
		t.Parallel()

		acceptCalled := make(chan struct{}, 1)

		listener := &mockListener{
			acceptFunc: func(context.Context) (Conn, error) {
				acceptCalled <- struct{}{}
				return nil, errors.New("listener closed")
			},
			closeFunc: func() error {
				return nil
			},
		}

		receiver := &Receiver{
			listen: func(*tls.Config, *quic.Config) (Listener, error) {
				return listener, nil
			},
		}

		err := receiver.Listen(&tls.Config{}, &quic.Config{})

		require.NoError(t, err)
		assert.Same(t, listener, receiver.listener)

		select {
		case <-acceptCalled:
		case <-time.After(receiverTestTimeout):
			t.Fatal("listener accept loop did not start")
		}
	})

	t.Run("errors if listener is already listening", func(t *testing.T) {
		t.Parallel()

		acceptCalled := make(chan struct{}, 1)

		listener := &mockListener{
			acceptFunc: func(context.Context) (Conn, error) {
				acceptCalled <- struct{}{}
				return nil, errors.New("listener closed")
			},
			closeFunc: func() error {
				return nil
			},
		}

		receiver := &Receiver{
			listen: func(*tls.Config, *quic.Config) (Listener, error) {
				return listener, nil
			},
		}

		assert.False(t, receiver.isListening)

		err := receiver.Listen(&tls.Config{}, &quic.Config{})
		require.NoError(t, err)
		assert.True(t, receiver.isListening)

		err = receiver.Listen(&tls.Config{}, &quic.Config{})
		require.Error(t, err)
		assert.True(t, receiver.isListening)

		select {
		case <-acceptCalled:
		case <-time.After(receiverTestTimeout):
			t.Fatal("listener accept loop did not start")
		}
	})

	t.Run("can listen again after cleanup", func(t *testing.T) {
		t.Parallel()

		firstAcceptCalled := make(chan struct{}, 1)
		secondAcceptCalled := make(chan struct{}, 1)

		firstListener := &mockListener{
			acceptFunc: func(context.Context) (Conn, error) {
				firstAcceptCalled <- struct{}{}
				return nil, errors.New("first listener closed")
			},
			closeFunc: func() error {
				return nil
			},
		}

		secondListener := &mockListener{
			acceptFunc: func(context.Context) (Conn, error) {
				secondAcceptCalled <- struct{}{}
				return nil, errors.New("second listener closed")
			},
			closeFunc: func() error {
				return nil
			},
		}

		listenCalls := 0

		receiver := &Receiver{
			listen: func(*tls.Config, *quic.Config) (Listener, error) {
				listenCalls++
				if listenCalls == 1 {
					return firstListener, nil
				}
				return secondListener, nil
			},
		}

		require.NoError(t, receiver.Listen(&tls.Config{}, &quic.Config{}))
		assert.True(t, receiver.isListening)

		select {
		case <-firstAcceptCalled:
		case <-time.After(receiverTestTimeout):
			t.Fatal("first listener accept loop did not start")
		}

		receiver.Cleanup()
		assert.False(t, receiver.isListening)

		require.NoError(t, receiver.Listen(&tls.Config{}, &quic.Config{}))
		assert.True(t, receiver.isListening)
		assert.Same(t, secondListener, receiver.listener)

		select {
		case <-secondAcceptCalled:
		case <-time.After(receiverTestTimeout):
			t.Fatal("second listener accept loop did not start")
		}
	})
}

func TestReceiverRecieve(t *testing.T) {
	t.Parallel()

	t.Run("registers accepted connection and handles it", func(t *testing.T) {
		t.Parallel()

		handlerStarted := make(chan struct{})
		releaseHandler := make(chan struct{})
		connectionClosed := make(chan struct{})

		conn := &mockConn{
			acceptStreamFunc: func(context.Context) (Stream, error) {
				close(handlerStarted)
				<-releaseHandler
				return nil, errors.New("connection closed")
			},
			closeWithErrFunc: func(
				quic.ApplicationErrorCode,
				string,
			) error {
				close(connectionClosed)
				return nil
			},
		}

		acceptCalls := 0

		listener := &mockListener{
			acceptFunc: func(context.Context) (Conn, error) {
				acceptCalls++

				if acceptCalls == 1 {
					return conn, nil
				}

				return nil, errors.New("listener closed")
			},
		}

		receiver := &Receiver{
			conns: make(map[Conn]struct{}),
		}

		receiver.recieve(listener)

		select {
		case <-handlerStarted:
		case <-time.After(receiverTestTimeout):
			t.Fatal("connection handler did not start")
		}

		receiver.mu.Lock()
		_, exists := receiver.conns[conn]
		receiver.mu.Unlock()

		require.True(t, exists, "accepted connection should be registered")

		close(releaseHandler)

		select {
		case <-connectionClosed:
		case <-time.After(receiverTestTimeout):
			t.Fatal("connection handler did not close connection")
		}

		require.Eventually(t, func() bool {
			receiver.mu.Lock()
			defer receiver.mu.Unlock()

			_, exists := receiver.conns[conn]
			return !exists
		}, receiverTestTimeout, time.Millisecond)
	})
}

func TestReceiverCleanup(t *testing.T) {
	t.Parallel()

	t.Run("nil resources do not panic", func(t *testing.T) {
		t.Parallel()

		receiver := &Receiver{}

		assert.NotPanics(t, func() {
			receiver.Cleanup()
		})
	})

	t.Run("closes listener and clears listening state", func(t *testing.T) {
		t.Parallel()

		closed := false

		listener := &mockListener{
			closeFunc: func() error {
				closed = true
				return nil
			},
			acceptFunc: func(context.Context) (Conn, error) {
				return nil, errors.New("unexpected accept")
			},
		}

		receiver := &Receiver{
			listener:    listener,
			isListening: true,
		}

		receiver.Cleanup()

		assert.True(t, closed)
		assert.False(t, receiver.isListening)
	})
}
