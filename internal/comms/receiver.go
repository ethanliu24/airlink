package comms

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/quic-go/quic-go"
)

var ReceiverAlreadyListeningError = errors.New("receiver is already listening")

type Receiver struct {
	listen      ListenFunc
	listener    Listener
	isListening bool
	conns       map[Conn]struct{}
	mu          sync.Mutex
}

func handleStream(stream Stream) {
	defer stream.Close()

	buf := make([]byte, 1024)
	for {
		n, err := stream.Read(buf)
		fmt.Printf("%s", string(buf[:n]))

		if errors.Is(err, io.EOF) {
			fmt.Println()
			break
		} else if err != nil {
			slog.Error("receiver stream handler failed", "err", err)
			break
		}
	}
}

func (r *Receiver) handleConnection(conn Conn) {
	defer func() {
		_ = conn.CloseWithError(0x0, "receiver connection closed normally")

		r.mu.Lock()
		defer r.mu.Unlock()

		delete(r.conns, conn)
	}()

	for {
		stream, err := conn.AcceptStream(context.Background())
		if err != nil {
			slog.Error("receiver connection handler failed", "err", err)
			break
		}

		go handleStream(stream)
	}
}

func (r *Receiver) recieve(listener Listener) {
	for {
		conn, err := listener.Accept(context.Background())
		if err != nil {
			slog.Error("receiver listener could not accept", "err", err)
			break
		}

		r.mu.Lock()
		r.conns[conn] = struct{}{}
		r.mu.Unlock()

		go r.handleConnection(conn)
	}
}

func (r *Receiver) Listen(tlsConfig *tls.Config, quicConfig *quic.Config) error {
	if r.isListening {
		return ReceiverAlreadyListeningError
	}

	r.isListening = true
	listener, err := r.listen(tlsConfig, quicConfig)
	if err != nil {
		return err
	}

	r.mu.Lock()
	r.listener = listener
	r.mu.Unlock()

	go r.recieve(listener)

	return nil
}

func (r *Receiver) Cleanup() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.listener != nil {
		r.listener.Close()
		r.listener = nil
	}

	for conn := range r.conns {
		_ = conn.CloseWithError(1, "sender tearing down")
		delete(r.conns, conn)
	}

	r.isListening = false
}

func NewReceiver(transport *quic.Transport) *Receiver {
	return &Receiver{
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
		conns:       make(map[Conn]struct{}),
	}
}
