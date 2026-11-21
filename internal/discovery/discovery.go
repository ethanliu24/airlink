package discovery

type Discovery interface {
	Start() error
	Stop() error
	Discover() ([]Peer, error)
}
