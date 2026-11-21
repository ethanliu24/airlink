package discovery

import "net"

type PeerId string

type Peer struct {
	Id      PeerId
	Name    string
	Address *net.UDPAddr
}

type Advertiser interface {
	Start() error
	Stop() error
}

type Querier interface {
	Discover() ([]Peer, error)
}

type Discovery interface {
	Advertiser
	Querier
}
