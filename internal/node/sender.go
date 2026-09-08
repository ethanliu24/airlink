package node

import (
	"github.com/quic-go/quic-go"
)

type Sender struct {
	transport *quic.Transport
}

func NewSender(transport *quic.Transport) *Sender {
	return &Sender{
		transport: transport,
	}
}
