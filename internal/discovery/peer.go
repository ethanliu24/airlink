package discovery

import (
	"airlink/internal/metadata"
	"net"
)

type Peer struct {
	Id      metadata.PeerId
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
