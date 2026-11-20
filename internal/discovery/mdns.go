package discovery

import (
	"errors"
	"io"
	"log"
	"net"
	"strings"
	"sync"

	"github.com/hashicorp/mdns"
)

var (
	ErrMDNSServiceNil       = errors.New("mdns service is nil")
	ErrMDNSDiscoveryStarted = errors.New("mdns discovery is already started")
)

const (
	MDNSServiceName       = "_airlink._udp"
	MDNSDomainName        = "local."
	mdnsEntriesBufferSize = 64
)

type DiscoveryMDNSFunc func(*net.UDPAddr) (Discovery, error)

type mdnsServer interface {
	Shutdown() error
}

type MDNSServiceEntry = mdns.ServiceEntry

type newMDNSServerFunc func(*mdns.Config) (mdnsServer, error)

type mdnsLookupFunc func(string, chan *MDNSServiceEntry) error

type MDNSDiscovery struct {
	service   *mdns.MDNSService
	server    mdnsServer
	newServer newMDNSServerFunc
	lookup    mdnsLookupFunc
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

func isAirlinkServiceEntry(entry *MDNSServiceEntry) bool {
    if entry == nil {
        return false
    }

    serviceSuffix := "." +
        strings.TrimSuffix(MDNSServiceName, ".") + "." +
        strings.TrimSuffix(MDNSDomainName, ".") + "."

    return strings.HasSuffix(
        strings.ToLower(entry.Name),
        strings.ToLower(serviceSuffix),
    )
}

func (d *MDNSDiscovery) Discover(entries chan *MDNSServiceEntry) error {
	discoveredEntries := make(chan *MDNSServiceEntry, mdnsEntriesBufferSize)
	filterDone := make(chan struct{})

	go func() {
		defer close(filterDone)

		for entry := range discoveredEntries {
			if !isAirlinkServiceEntry(entry) {
				continue
			}

			select {
			case entries <- entry:
			default:
			}
		}
	}()

	err := d.lookup(MDNSServiceName, discoveredEntries)

	close(discoveredEntries)
	<-filterDone

	return err
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

	return newMDNSDiscovery(
		service,
		func(config *mdns.Config) (mdnsServer, error) {
			config.Logger = log.New(io.Discard, "", 0)
			return mdns.NewServer(config)
		},
		func(service string, entries chan *MDNSServiceEntry) error {
			return mdns.Lookup(service, entries)
		},
	), nil
}

func newMDNSDiscovery(
	service *mdns.MDNSService,
	newServer newMDNSServerFunc,
	lookup mdnsLookupFunc,
) *MDNSDiscovery {
	return &MDNSDiscovery{
		service:   service,
		newServer: newServer,
		lookup:    lookup,
	}
}
