package transport

import (
	"airlink/internal/metadata"
	"airlink/internal/security"
	"airlink/internal/transfer"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/quic-go/quic-go"
)

var ReceiverAlreadyListeningError = errors.New("receiver is already listening")

type Listener interface {
	Accept(context.Context) (Conn, error)
	Close() error
}

type listenFunc func(*tls.Config, *quic.Config) (Listener, error)

type Receiver interface {
	Listen(*tls.Config, *quic.Config) error
	CleanUp()
}

type receiveFunc func(stream transfer.TransferStream) (transfer.TransferId, error)

type receiver struct {
	id          metadata.PeerId
	listen      listenFunc
	listener    Listener
	isListening bool
	receive     receiveFunc
	conns       map[Conn]struct{}
	mu          sync.Mutex
}

func (r *receiver) handleStream(stream transfer.TransferStream) {
	r.receive(stream)
}

func (r *receiver) handleConnection(conn Conn) {
	defer func() {
		_ = conn.CloseWithError(0x0, "receiver connection closed normally")

		r.mu.Lock()
		defer r.mu.Unlock()

		delete(r.conns, conn)
	}()

	for {
		stream, err := conn.AcceptStream(context.Background())
		if err != nil {
			slog.Error("receiver connection closed", "err", err, "errType", fmt.Sprintf("%T", err))
			break
		}

		go r.handleStream(stream)
	}
}

func (r *receiver) authenticatePeer(conn Conn) (*security.PeerIdentity, error) {
	ctx, cancel := context.WithTimeout(context.Background(), maxHandshakeTimeoutSeconds)
	defer cancel()

	authStream, err := conn.AcceptStream(ctx)
	if err != nil {
		return nil, fmt.Errorf("accept auth stream: %w", err)
	}

	defer authStream.Close()

	if deadline, ok := ctx.Deadline(); ok {
		if err := authStream.SetDeadline(deadline); err != nil {
			return nil, fmt.Errorf("set auth stream deadline: %w", err)
		}
	}

	peerIdentity, err := exchangePeerIdentity(
		authStream,
		r.id,
		conn.ConnectionState(),
	)
	if err != nil {
		return nil, fmt.Errorf("exchange peer identity: %w", err)
	}

	return peerIdentity, nil
}

func (r *receiver) recieve(listener Listener) {
	for {
		conn, err := listener.Accept(context.Background())
		if err != nil {
			slog.Error("receiver listener could not accept", "err", err)
			break
		}

		_, err = r.authenticatePeer(conn)
		if err != nil {
			slog.Error("peer authentication failed", "err", err)
			_ = conn.CloseWithError(0, "peer identity exchange failed")
			continue
		}

		r.mu.Lock()
		r.conns[conn] = struct{}{}
		r.mu.Unlock()

		go r.handleConnection(conn)
	}
}

func (r *receiver) Listen(tlsConfig *tls.Config, quicConfig *quic.Config) error {
	r.mu.Lock()
	if r.isListening {
		r.mu.Unlock()
		return ReceiverAlreadyListeningError
	}
	r.isListening = true
	r.mu.Unlock()

	listener, err := r.listen(tlsConfig, quicConfig)
	if err != nil {
		r.mu.Lock()
		r.isListening = false
		r.mu.Unlock()
		return err
	}

	r.mu.Lock()
	r.listener = listener
	r.mu.Unlock()

	go r.recieve(listener)

	return nil
}

func (r *receiver) CleanUp() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.listener != nil {
		r.listener.Close()
		r.listener = nil
	}

	for conn := range r.conns {
		_ = conn.CloseWithError(1, "receiver tearing down")
		delete(r.conns, conn)
	}

	r.isListening = false
}

func NewReceiver(id metadata.PeerId, transport *quic.Transport, receive receiveFunc) Receiver {
	return &receiver{
		id: id,
		listen: func(tlsConfig *tls.Config, quicConfig *quic.Config) (Listener, error) {
			listener, err := transport.Listen(tlsConfig, quicConfig)
			if err != nil {
				return nil, err
			}

			return &quicListener{
				listener: listener,
			}, nil
		},
		listener:    nil,
		isListening: false,
		receive:     receive,
		conns:       make(map[Conn]struct{}),
	}
}
