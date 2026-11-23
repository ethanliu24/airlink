package transport

import (
	"airlink/internal/metadata"
	"airlink/internal/transfer"
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"sync"

	"github.com/quic-go/quic-go"
)

type Sender interface {
	Send(*net.UDPAddr, string, *tls.Config, *quic.Config) error
	CleanUp()
}

type dialFunc func(context.Context, net.Addr, *tls.Config, *quic.Config) (Conn, error)

type sendFunc func(stream transfer.TransferStream, filename string) (transfer.TransferId, error)

type sender struct {
	id    metadata.PeerId
	dial  dialFunc
	mu    sync.Mutex
	conns map[string]Conn
	send  sendFunc
}

func (s *sender) sendOverStream(stream transfer.TransferStream, filename string) error {
	_, err := s.send(stream, filename)
	if err != nil {
		return err
	}

	return nil
}

func (s *sender) sendFile(conn Conn, filename string) {
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
		if err := s.sendOverStream(stream, filename); err != nil {
			slog.Error("sender failed to send data", "err", err)
		}
	}()
}

func (s *sender) Send(
	receiverAddr *net.UDPAddr,
	filename string,
	tlsConfig *tls.Config,
	quicConfig *quic.Config,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), maxHandshakeTimeoutSeconds)
	defer cancel()

	addr := receiverAddr.String()

	conn, err := s.dial(ctx, receiverAddr, tlsConfig, quicConfig)
	if err != nil {
		return err
	}

	authStream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		_ = conn.CloseWithError(0x0, "peer identity stream failed")
		return err
	}

	defer authStream.Close()

	if deadline, ok := ctx.Deadline(); ok {
		if err := authStream.SetDeadline(deadline); err != nil {
			_ = conn.CloseWithError(0, "peer identity setup failed")
			return err
		}
	}

	_, err = exchangePeerIdentity(authStream, s.id, conn.ConnectionState())
	if err != nil {
		conn.CloseWithError(0, "peer identity exchange failed")
		return err
	}

	s.mu.Lock()
	s.conns[addr] = conn
	s.mu.Unlock()

	go s.sendFile(conn, filename)

	return nil
}

func (s *sender) CleanUp() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for addr, conn := range s.conns {
		_ = conn.CloseWithError(0x0, "sender connection closed gracefully")
		delete(s.conns, addr)
	}
}

func NewSender(id metadata.PeerId, transport *quic.Transport, send sendFunc) Sender {
	return &sender{
		id: id,
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
		send:  send,
	}
}
