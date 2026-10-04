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

type Reciever struct {
	listen ListenFunc
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
			slog.Error("reciever stream handler failed", "err", err)
			break
		}
	}
}

func handleConnection(conn Conn) {
	defer conn.CloseWithError(0x0, "reciever connection closed normally")

	for {
		stream, err := conn.AcceptStream(context.Background())
		if err != nil {
			slog.Error("reciever connection handler failed", "err", err)
			break
		}

		go handleStream(stream)
	}
}

func recieve(listener Listener) {
	for {
		conn, err := listener.Accept(context.Background())
		if err != nil {
			slog.Error("reciever listener could not accept", "err", err)
			break
		}

		go handleConnection(conn)
	}
}

func (r *Reciever) Listen(tlsConfig *tls.Config, quicConfig *quic.Config) error {
	listener, err := r.listen(tlsConfig, quicConfig)
	if err != nil {
		return err
	}

	go recieve(listener)

	return nil
}

func (r *Reciever) Cleanup() {
	// TODO
}

func NewReciever(transport *quic.Transport) *Reciever {
	return &Reciever{
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

