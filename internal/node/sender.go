package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/quic-go/quic-go"
)

var ErrorSenderConnectionInitialization = fmt.Errorf("sender stream write failed")

type Sender struct {
	transport *quic.Transport
}

func sendData(stream *quic.Stream, data []byte) {
	defer stream.Close()

	n, err := stream.Write(data)
	if err != nil {
		slog.Error("sender stream write failed", "bytesWritten", n, "err", err)
		return
	}

	slog.Error("send successful", "bytesWritten", n)
}

func send(conn *quic.Conn, data []byte) {
	stream, err := conn.OpenStream()
	if errors.Is(err, &quic.StreamLimitReachedError{}) {
		slog.Error("sender stream limit reached", "err", err)
		return
	} else if err != nil {
		slog.Error("sender stream open failed", "err", err)
		return
	}

	go sendData(stream, data)
}

// TODO stream the data instead of loading all into memory
// TODO figure out sending multiple data to the same address, maybe cache it in map[*netUDPAddr]*quic.Conn
func (s *Sender) Send(recieverAddr *net.UDPAddr, data []byte) error {
	// TODO refactor constants to config
	ctx, cancel := context.WithTimeout(context.Background(), 3 * time.Second) // 3s handshake timeout
	defer cancel()

	conn, err := s.transport.Dial(ctx, recieverAddr, tlsConfig, quicConfig)
	if err != nil {
		return ErrorSenderConnectionInitialization
	}

	defer conn.CloseWithError(0x0, "sender connection closed gracefully")

	go send(conn, data)
	return nil
}

func NewSender(transport *quic.Transport) *Sender {
	return &Sender{
		transport: transport,
	}
}
