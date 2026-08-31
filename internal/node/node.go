package node

import (
	"fmt"
	"net"
	"log/slog"

	"github.com/quic-go/quic-go"
)

const IP_ADDRESS = "127.0.0.1"

type P2PNode struct {
	transport *quic.Transport
}

func toUDPAddress(port int) (*net.UDPAddr, error) {
	address := fmt.Sprintf("%s:%d", IP_ADDRESS, port)
    return net.ResolveUDPAddr("udp", address)
}

func NewP2PNode(port int) (*P2PNode, error) {
	addr, err := toUDPAddress(port)
	if err != nil {
		slog.Error("could not resolve UDP to address", "port", port, "err", err)
		return nil, err
	}

	udpConn, err := net.ListenUDP("udp", addr)
	if err != nil {
		slog.Error("could not listen on UDP address", "address", addr, "err", err)
		return nil, err
	}

	transport := &quic.Transport{Conn: udpConn}

	return &P2PNode{
		transport: transport,
	}, nil
}
