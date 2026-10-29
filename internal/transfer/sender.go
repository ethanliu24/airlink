package transfer

import (
	"airlink/internal/file"
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
var FILE_READ_CHUNK_SIZE_BYTES = 1024 * 64

type Sender struct {
	dial       DialFunc
	openReader file.OpenReaderFunc
	mu         sync.Mutex
	conns      map[string]Conn
	addr       net.Addr
}

func (s *Sender) sendOverStream(stream Stream, filename string) error {
	defer stream.Close()

	reader, err := s.openReader(filename)
	if err != nil {
		return err
	}

	defer reader.Close()

	send := newTransferSend(reader, stream, s.addr)
	result := send.run()

	if result.Err != nil {
		return result.Err
	}

	// TODO handle this
	if !result.Accepted {
		if result.RejectReason != nil {
		}
	}

	return nil
}

func (s *Sender) sendFile(conn Conn, filename string) {
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

func (s *Sender) Send(
	receiverAddr *net.UDPAddr,
	filename string,
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

	go s.sendFile(conn, filename)

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

func NewSender(transport *quic.Transport, openReader file.OpenReaderFunc, addr net.Addr) *Sender {
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
		conns:      make(map[string]Conn),
		openReader: openReader,
		addr:       addr,
	}
}
