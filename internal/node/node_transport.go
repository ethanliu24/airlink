package node

import "net"

func (n *P2PNode) SendFile(receiverAddr *net.UDPAddr, filename string) error {
	return n.sender.Send(receiverAddr, filename, n.clientTLS, n.quicConfig)
}

func (n *P2PNode) listen() error {
	return n.receiver.Listen(n.serverTLS, n.quicConfig)
}
