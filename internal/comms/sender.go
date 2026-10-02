package comms

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"sync"

	"github.com/quic-go/quic-go"
)

type Sender struct {
	dial DialFunc

	mu    sync.Mutex
	conns map[string]Conn
}

func sendOverStream(stream Stream, data []byte) error {
	defer stream.Close()

	n, err := stream.Write(data)
	if err != nil {
		slog.Error("sender stream write failed", "bytesWritten", n, "err", err)

		return err
	}

	slog.Debug("send successful", "bytesWritten", n)
	return nil
}

func (s *Sender) sendData(addr string, conn Conn, data []byte) {
	defer func() {
		_ = conn.CloseWithError(0, "sender connection closed gracefully")

		s.mu.Lock()
		defer s.mu.Unlock()

		if current, ok := s.conns[addr]; ok && current == conn {
			delete(s.conns, addr)
		}
	}()

	stream, err := conn.OpenStream()
	if err != nil {
		if errors.Is(err, &quic.StreamLimitReachedError{}) {
			slog.Error("sender stream limit reached", "err", err)
		} else {
			slog.Error("sender stream open failed", "err", err)
		}

		return
	}

	go func() {
		if err := sendOverStream(stream, data); err != nil {
			slog.Error("sender failed to send data", "err", err)
		}
	}()
}

func (s *Sender) Send(receiverAddr *net.UDPAddr, data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), MAX_HANDSHAKE_TIMEOUT_SECONDS)
	defer cancel()

	addr := receiverAddr.String()

	conn, err := s.dial(ctx, receiverAddr, generateTLSConfig(), getQuicConfig())
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.conns[addr] = conn
	s.mu.Unlock()

	go s.sendData(addr, conn, data)

	return nil
}

func (s *Sender) Cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for addr, conn := range s.conns {
		_ = conn.CloseWithError(1, "sender tearing down")
		delete(s.conns, addr)
	}
}

func NewSender(transport *quic.Transport) *Sender {
	return &Sender{
		dial: func(ctx context.Context, addr net.Addr, tlsConf *tls.Config, conf *quic.Config) (Conn, error) {
			conn, err := transport.Dial(ctx, addr, tlsConf, conf)
			if err != nil {
				return nil, err
			}

			return &quicConn{
				conn: conn,
			}, nil
		},
		conns: make(map[string]Conn),
	}
}
