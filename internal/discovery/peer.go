package discovery

import "net"

type PeerID string

type Peer struct {
	ID        PeerID
	Name      string
	Address *net.UDPAddr
}
