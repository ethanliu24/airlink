package comms

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/quic-go/quic-go"
)

type Receiver struct {
	listen ListenFunc
	conn   Conn
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

func handleConnection(conn Conn) {
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

		r.conn = conn
		go handleConnection(conn)
	}
}

func (r *Receiver) Listen(tlsConfig *tls.Config, quicConfig *quic.Config) error {
	listener, err := r.listen(tlsConfig, quicConfig)
	if err != nil {
		return err
	}

	go r.recieve(listener)

	return nil
}

func (r *Receiver) Cleanup() {
	if r.conn != nil {
		r.conn.CloseWithError(0x0, "receiver connection closed normally")
	}
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
	}
}
