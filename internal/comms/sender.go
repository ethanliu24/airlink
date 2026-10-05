package comms

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

var MAX_HANDSHAKE_TIMEOUT_SECONDS = 3 * time.Second

type Sender struct {
	dial  DialFunc
	mu    sync.Mutex
	conns map[string]Conn
}

func sendOverStream(stream Stream, data []byte) error {
	defer stream.Close()

	n, err := stream.Write(data)
	if err != nil {
		slog.Error("sender sendOverStream write failed", "bytesWritten", n, "err", err)
		return err
	}

	slog.Debug("send successful", "bytesWritten", n)
	return nil
}

func (s *Sender) sendData(conn Conn, data []byte) {
	stream, err := conn.OpenStream()
	if err != nil {
		if errors.Is(err, &quic.StreamLimitReachedError{}) {
			slog.Error("sender stream limit reached", "err", err)
		} else {
			slog.Error("sender streamData open failed", "err", err)
		}

		return
	}

	go func() {
		if err := sendOverStream(stream, data); err != nil {
			slog.Error("sender failed to send data", "err", err)
		}
	}()
}

func (s *Sender) Send(
	receiverAddr *net.UDPAddr,
	data []byte,
	tlsConfig *tls.Config,
	quicConfig *quic.Config,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), MAX_HANDSHAKE_TIMEOUT_SECONDS)
	defer cancel()

	addr := receiverAddr.String()

	conn, err := s.dial(ctx, receiverAddr, tlsConfig, quicConfig)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.conns[addr] = conn
	s.mu.Unlock()

	go s.sendData(conn, data)

	return nil
}

func (s *Sender) Cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for addr, conn := range s.conns {
		_ = conn.CloseWithError(0x0, "sender connection closed gracefully")
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
