package node

import (
	"log/slog"
	"net"

	"airlink/internal/comms"
	"github.com/quic-go/quic-go"
)

const IP_ADDRESS = "127.0.0.1"

// TODO transport cleanup
type P2PNode struct {
	transport *quic.Transport
	sender    *comms.Sender
	addr      net.Addr
}

func (n *P2PNode) Send(recieverAddr *net.UDPAddr, data []byte) {
	n.sender.Send(recieverAddr, data)
}

func (n *P2PNode) Cleanup() {
	defer n.sender.Cleanup()
	defer n.transport.Close()
}

type UDPListenFunc func(network string, address *net.UDPAddr) (*net.UDPConn, error)

func NewP2PNode(addr *net.UDPAddr, listen UDPListenFunc) (*P2PNode, error) {
	udpConn, err := listen(addr.Network(), addr)
	if err != nil {
		slog.Error("could not listen on UDP address", "address", addr, "err", err)
		return nil, err
	}

	transport := &quic.Transport{Conn: udpConn}
	sender := comms.NewSender(transport)

	return &P2PNode{
		transport: transport,
		sender:    sender,
		addr:      addr,
	}, nil
}
