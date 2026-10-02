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

type DialFunc func(ctx context.Context, addr net.Addr, tlsConf *tls.Config, conf *quic.Config) (*quic.Conn, error)

type Sender struct {
	transport *quic.Transport
	dial      DialFunc
	mu        sync.Mutex
	conns     map[string]*quic.Conn
}

func sendOverStream(stream *quic.Stream, data []byte) {
	defer stream.Close()

	n, err := stream.Write(data)
	if err != nil {
		slog.Error("sender stream write failed", "bytesWritten", n, "err", err)
		return
	}

	slog.Error("send successful", "bytesWritten", n)
}

func sendData(s *Sender, addr string, conn *quic.Conn, data []byte) {
	defer func() {
		conn.CloseWithError(0x0, "sender connection closed gracefully")
		s.mu.Lock()
		delete(s.conns, addr)
		s.mu.Unlock()
	}()

	stream, err := conn.OpenStream()
	if errors.Is(err, &quic.StreamLimitReachedError{}) {
		slog.Error("sender stream limit reached", "err", err)
		return
	} else if err != nil {
		slog.Error("sender stream open failed", "err", err)
		return
	}

	go sendOverStream(stream, data)
}

// TODO stream the data instead of loading all into memory
// TODO figure out sending multiple data to the same address, maybe cache it in map[*netUDPAddr]*quic.Conn
func (s *Sender) Send(recieverAddr *net.UDPAddr, data []byte) error {
	// TODO refactor constants to config
	ctx, cancel := context.WithTimeout(context.Background(), MAX_HANDSHAKE_TIMEOUT_SECONDS)
	defer cancel()

	addrStr := recieverAddr.String()
	conn, err := s.dial(ctx, recieverAddr, generateTLSConfig(), getQuicConfig())
	if err != nil {
		return err
	}

	// Trace connection for clean up later
	s.mu.Lock()
	s.conns[addrStr] = conn
	s.mu.Unlock()

	go sendData(s, addrStr, conn, data)
	return nil
}

func (s *Sender) Cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for addr, conn := range s.conns {
		_ = conn.CloseWithError(0x1, "sender tearing down")
		delete(s.conns, addr)
	}
}

// Caller is responsible for cleaning up transport
func NewSender(transport *quic.Transport) *Sender {
	return &Sender{
		transport: transport,
		dial:      transport.Dial,

		conns:     make(map[string]*quic.Conn),
	}
}
