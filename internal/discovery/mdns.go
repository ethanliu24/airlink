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
	mdnsEntriesBufferSize = 64 // TODO make this configurable and changable during runtime
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

func (d *MDNSDiscovery) Discover() ([]Peer, error) {
    entries := make(chan *MDNSServiceEntry, mdnsEntriesBufferSize)
    lookupDone := make(chan error, 1)

    go func() {
        err := d.lookup(MDNSServiceName, entries)
        close(entries)
        lookupDone <- err
    }()

    peers := make([]Peer, 0)
    for entry := range entries {
        peer, ok := mdnsEntryToPeer(entry)
        if !ok {
            continue
        }

        peers = append(peers, *peer)
    }

    err := <-lookupDone

    return peers, err
}

func mdnsInstanceName(name string) (string, bool) {
    suffix :=
		"." +
        strings.TrimSuffix(MDNSServiceName, ".") + "." +
        strings.TrimSuffix(MDNSDomainName, ".") + "."

    if len(name) <= len(suffix) {
        return "", false
    }

    if !strings.EqualFold(name[len(name)-len(suffix):], suffix) {
        return "", false
    }

    return name[:len(name)-len(suffix)], true
}

func mdnsEntryToPeer(entry *MDNSServiceEntry) (*Peer, bool) {
    if entry == nil {
        return nil, false
    }

    name, ok := mdnsInstanceName(entry.Name)
    if !ok {
        return nil, false
    }

    // Prefer IPv4, falling back to IPv6.
    ip := entry.AddrV4
    if len(ip) == 0 {
        ip = entry.AddrV6
    }

    if len(ip) == 0 {
        return nil, false
    }

    return &Peer{
        Name: name,
        Address: &net.UDPAddr{
            IP:   append(net.IP(nil), ip...),
            Port: entry.Port,
        },
    }, true
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
