package discovery

import (
	"errors"
	"net"
	"sync"

	"github.com/hashicorp/mdns"
)

var (
	ErrMDNSServiceNil       = errors.New("mdns service is nil")
	ErrMDNSDiscoveryStarted = errors.New("mdns discovery is already started")
)

const (
	MDNSServiceName = "_airlink._udp"
	MDNSDomainName  = "local."
)

type DiscoveryMDNSFunc func(*net.UDPAddr) (Discovery, error)

type mdnsServer interface {
	Shutdown() error
}

type newMDNSServerFunc func(*mdns.Config) (mdnsServer, error)

type MDNSDiscovery struct {
	service   *mdns.MDNSService
	server    mdnsServer
	newServer newMDNSServerFunc
	mu        sync.Mutex
}

func (d *MDNSDiscovery) Start() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.service == nil {
		return ErrMDNSServiceNil
	}

	if d.server != nil {
		return ErrMDNSDiscoveryStarted
	}

	server, err := d.newServer(&mdns.Config{Zone: d.service})
	if err != nil {
		return err
	}

	d.server = server
	return nil
}

func (d *MDNSDiscovery) Stop() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.server == nil {
		return nil
	}

	err := d.server.Shutdown()
	if err != nil {
		return err
	}

	d.server = nil
	return nil
}

func NewMDNSDiscovery(
	name string,
	serviceName string,
	domainName string,
	hostName string,
	port int,
	ips []net.IP,
	info []string,
) (*MDNSDiscovery, error) {
	service, err := mdns.NewMDNSService(name, serviceName, domainName, hostName, port, ips, info)
	if err != nil {
		return nil, err
	}

	return newMDNSDiscovery(service, func(config *mdns.Config) (mdnsServer, error) {
		return mdns.NewServer(config)
	}), nil
}

func newMDNSDiscovery(service *mdns.MDNSService, newServer newMDNSServerFunc) *MDNSDiscovery {
	return &MDNSDiscovery{
		service:   service,
		newServer: newServer,
	}
}
