package node

import (
	"crypto/tls"
	"log/slog"
	"net"

	"airlink/internal/comms"

	"github.com/quic-go/quic-go"
)

const IP_ADDRESS = "127.0.0.1"

// TODO transport cleanup
// TODO refactor
type P2PNode struct {
	transport  *quic.Transport
	receiver   *comms.Receiver
	sender     *comms.Sender
	addr       net.Addr
	tlsConfig  *tls.Config
	quicConfig *quic.Config
}

func (n *P2PNode) Listen() error {
	return n.receiver.Listen(n.tlsConfig, n.quicConfig)
}

func (n *P2PNode) Send(receiverAddr *net.UDPAddr, data []byte) error {
	return n.sender.Send(receiverAddr, data, n.tlsConfig, n.quicConfig)
}

func (n *P2PNode) Cleanup() {
	defer n.receiver.Cleanup()
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

	tlsConfig := generateTLSConfig()
	quicConfig := getQuicConfig()

	transport := &quic.Transport{Conn: udpConn}
	receiver := comms.NewReceiver(transport)
	sender := comms.NewSender(transport)

	return &P2PNode{
		transport:  transport,
		receiver:   receiver,
		sender:     sender,
		addr:       addr,
		tlsConfig:  tlsConfig,
		quicConfig: quicConfig,
	}, nil
}
