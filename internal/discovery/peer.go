package discovery

import "net"

type PeerId string

type Peer struct {
	Id      PeerId
	Name    string
	Address *net.UDPAddr
}

type Discovery interface {
	Start() error
	Stop() error
	Discover() ([]Peer, error)
}
