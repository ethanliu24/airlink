package discovery

import (
	"errors"
	"net"
	"testing"

	"github.com/hashicorp/mdns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockMDNSServer struct {
	shutdownCalls int
	shutdownErr   error
}

func (s *mockMDNSServer) Shutdown() error {
	s.shutdownCalls++
	return s.shutdownErr
}

func TestMDNSDiscoveryStartStopLifecycle(t *testing.T) {
	server := &mockMDNSServer{}
	newServerCalls := 0
	discovery := newMDNSDiscovery(
		&mdns.MDNSService{},
		func(config *mdns.Config) (mdnsServer, error) {
			newServerCalls++
			assert.NotNil(t, config.Zone)
			return server, nil
		},
		func(s string, c chan *MDNSServiceEntry) error {
			return nil
		},
	)

	require.NoError(t, discovery.Stop(), "stopping before start must be safe")
	require.NoError(t, discovery.Start())
	require.ErrorIs(t, discovery.Start(), ErrMDNSDiscoveryStarted)
	require.NoError(t, discovery.Stop())
	require.NoError(t, discovery.Stop(), "stopping twice must be safe")

	assert.Equal(t, 1, newServerCalls)
	assert.Equal(t, 1, server.shutdownCalls)
}

func TestMDNSDiscoveryStartRejectsNilService(t *testing.T) {
	discovery := newMDNSDiscovery(
		nil,
		func(*mdns.Config) (mdnsServer, error) {
			t.Fatal("server must not be created without a service")
			return nil, nil
		},
		func(s string, c chan *MDNSServiceEntry) error {
			return nil
		},
	)

	require.ErrorIs(t, discovery.Start(), ErrMDNSServiceNil)
}

func TestMDNSDiscoveryStopCanRetryAfterShutdownFailure(t *testing.T) {
	shutdownErr := errors.New("shutdown failed")
	server := &mockMDNSServer{shutdownErr: shutdownErr}
	discovery := newMDNSDiscovery(
		&mdns.MDNSService{},
		func(*mdns.Config) (mdnsServer, error) {
			return server, nil
		},
		func(s string, c chan *MDNSServiceEntry) error {
			return nil
		},
	)
	require.NoError(t, discovery.Start())

	require.ErrorIs(t, discovery.Stop(), shutdownErr)
	server.shutdownErr = nil
	require.NoError(t, discovery.Stop())
	assert.Equal(t, 2, server.shutdownCalls)
}

func TestMDNSDiscoveryDiscoverConvertsEntriesToPeers(t *testing.T) {
    discovery := newMDNSDiscovery(
        &mdns.MDNSService{},
        nil,
        func(service string, entries chan *MDNSServiceEntry) error {
            assert.Equal(t, MDNSServiceName, service)

            entries <- &MDNSServiceEntry{
                Name:   "SomeDevice._airlink._udp.local.",
                AddrV4: net.ParseIP("192.0.2.10"),
                AddrV6: net.ParseIP("2001:db8::1"),
                Port:   1111,
            }

            return nil
        },
    )

    peers, err := discovery.Discover()

    require.NoError(t, err)
    require.Len(t, peers, 1)

    assert.Equal(t, Peer{
        Name: "SomeDevice",
        Address: &net.UDPAddr{
            IP:   net.ParseIP("192.0.2.10"),
            Port: 1111,
        },
    }, peers[0])
}

func TestMDNSDiscoveryDiscoverFallsBackToIPv6(t *testing.T) {
    discovery := newMDNSDiscovery(
        &mdns.MDNSService{},
        nil,
        func(string, chan *MDNSServiceEntry) error {
            return nil
        },
    )

    // Use a separate test to supply the IPv6 entry.
    discovery.lookup = func(_ string, entries chan *MDNSServiceEntry) error {
        entries <- &MDNSServiceEntry{
            Name:   "SomeDevice._airlink._udp.local.",
            AddrV6: net.ParseIP("2001:db8::1"),
            Port:   1111,
        }
        return nil
    }

    peers, err := discovery.Discover()

    require.NoError(t, err)
    require.Len(t, peers, 1)

    assert.Equal(t, net.ParseIP("2001:db8::1"), peers[0].Address.IP)
    assert.Equal(t, 1111, peers[0].Address.Port)
}

func TestMDNSDiscoveryDiscoverFiltersInvalidEntries(t *testing.T) {
    discovery := newMDNSDiscovery(
        &mdns.MDNSService{},
        nil,
        func(_ string, entries chan *MDNSServiceEntry) error {
            entries <- nil

            entries <- &MDNSServiceEntry{
                Name: "SomeDevice._airplay._tcp.local.",
                AddrV4: net.ParseIP("192.0.2.10"),
                Port: 1111,
            }

            entries <- &MDNSServiceEntry{
                Name: "SomeDevice._airlink._tcp.local.",
                AddrV4: net.ParseIP("192.0.2.10"),
                Port: 1111,
            }

            // Valid Airlink service, but no usable address.
            entries <- &MDNSServiceEntry{
                Name: "NoAddress._airlink._udp.local.",
                Port: 1111,
            }

            return nil
        },
    )

    peers, err := discovery.Discover()

    require.NoError(t, err)
    assert.Empty(t, peers)
}

func TestMDNSDiscoveryDiscoverReturnsLookupError(t *testing.T) {
    lookupErr := errors.New("lookup failed")

    discovery := newMDNSDiscovery(
        &mdns.MDNSService{},
        nil,
        func(string, chan *MDNSServiceEntry) error {
            return lookupErr
        },
    )

    peers, err := discovery.Discover()

    require.ErrorIs(t, err, lookupErr)
    assert.Empty(t, peers)
}

func TestMDNSDiscoveryDiscoverFiltersEntriesByService(t *testing.T) {
    airlinkEntry := &MDNSServiceEntry{
        Name:   "SomeDevice._airlink._udp.local.",
        AddrV4: net.ParseIP("192.0.2.10"),
        Port:   1111,
    }

    uppercaseAirlinkEntry := &MDNSServiceEntry{
        Name:   "AnotherDevice._AIRLINK._UDP.LOCAL.",
        AddrV4: net.ParseIP("192.0.2.20"),
        Port:   2222,
    }

    lookupCalls := 0

    discovery := newMDNSDiscovery(
        &mdns.MDNSService{},
        nil,
        func(service string, discovered chan *MDNSServiceEntry) error {
            lookupCalls++

            assert.Equal(t, MDNSServiceName, service)

            discovered <- nil
            discovered <- &MDNSServiceEntry{
                Name: "SomeDevice._airplay._tcp.local.",
            }
            discovered <- &MDNSServiceEntry{
                Name: "SomeDevice._companion-link._tcp.local.",
            }
            discovered <- &MDNSServiceEntry{
                Name: "SomeDevice._airlink._tcp.local.",
            }
            discovered <- airlinkEntry
            discovered <- uppercaseAirlinkEntry

            return nil
        },
    )

    peers, err := discovery.Discover()

    require.NoError(t, err)
    assert.Equal(t, 1, lookupCalls)

    require.Len(t, peers, 2)

    assert.Equal(t, Peer{
        Name: "SomeDevice",
        Address: &net.UDPAddr{
            IP:   net.ParseIP("192.0.2.10"),
            Port: 1111,
        },
    }, peers[0])

    assert.Equal(t, Peer{
        Name: "AnotherDevice",
        Address: &net.UDPAddr{
            IP:   net.ParseIP("192.0.2.20"),
            Port: 2222,
        },
    }, peers[1])
}
