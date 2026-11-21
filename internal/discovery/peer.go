package discovery

import "net"

type PeerId string

type Peer struct {
	Id      PeerId
	Name    string
	Address *net.UDPAddr
}
