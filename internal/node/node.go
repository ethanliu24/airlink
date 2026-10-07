package node

import (
	"crypto/tls"
	"log/slog"
	"net"

	"airlink/internal/file"
	"airlink/internal/transfer"

	"github.com/quic-go/quic-go"
)

const IP_ADDRESS = "127.0.0.1"

type P2PNode struct {
	transport  *quic.Transport
	receiver   *transfer.Receiver
	sender     *transfer.Sender
	addr       net.Addr
	tlsConfig  *tls.Config
	quicConfig *quic.Config
}

func (n *P2PNode) Listen() error {
	return n.receiver.Listen(n.tlsConfig, n.quicConfig)
}

func (n *P2PNode) SendFile(receiverAddr *net.UDPAddr, filename string) error {
	return n.sender.Send(receiverAddr, filename, n.tlsConfig, n.quicConfig)
}

func (n *P2PNode) Cleanup() {
	n.receiver.Cleanup()
	n.sender.Cleanup()
	n.transport.Close()
}

func NewP2PNode(addr *net.UDPAddr) (*P2PNode, error) {
	return newP2PNode(addr, net.ListenUDP, file.OpenReader)
}

type udpListenFunc func(network string, address *net.UDPAddr) (*net.UDPConn, error)

func newP2PNode(
	addr *net.UDPAddr,
	listen udpListenFunc,
	openReader file.OpenReaderFunc,
) (*P2PNode, error) {
	udpConn, err := listen(addr.Network(), addr)
	if err != nil {
		slog.Error("could not listen on UDP address", "address", addr, "err", err)
		return nil, err
	}

	tlsConfig := generateTLSConfig()
	quicConfig := getQuicConfig()

	transport := &quic.Transport{Conn: udpConn}
	receiver := transfer.NewReceiver(transport)
	sender := transfer.NewSender(transport, openReader)

	return &P2PNode{
		transport:  transport,
		receiver:   receiver,
		sender:     sender,
		addr:       addr,
		tlsConfig:  tlsConfig,
		quicConfig: quicConfig,
	}, nil
}
