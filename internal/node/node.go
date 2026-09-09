package node

import (
	"log/slog"
	"net"

	"github.com/quic-go/quic-go"
)

const IP_ADDRESS = "127.0.0.1"

// TODO transport cleanup
type P2PNode struct {
	Sender    *Sender
	addr      net.Addr
}

func (n *P2PNode) Cleanup() {
	// TODO n.Sender.Close() close all currently open connections
	// defer n.transport.Close()
}

type UDPListenFunc func(network string, address *net.UDPAddr) (*net.UDPConn, error)

func NewP2PNode(addr *net.UDPAddr, listen UDPListenFunc) (*P2PNode, error) {
	udpConn, err := listen(addr.Network(), addr)
	if err != nil {
		slog.Error("could not listen on UDP address", "address", addr, "err", err)
		return nil, err
	}

	transport := &quic.Transport{Conn: udpConn}
	sender := newSender(transport)

	return &P2PNode{
		Sender:    sender,
		addr:      addr,
	}, nil
}
