package discovery

type Discovery interface {
	Start() error
	Stop() error
	Discover(chan MDNSServiceEntry) error
}
