package discovery

import (
	"errors"
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

func TestMDNSDiscoveryDiscoverForwardsValidEntries(t *testing.T) {
    expected := &MDNSServiceEntry{
        Name: "SomeDevice._airlink._udp.local.",
    }

    discovery := newMDNSDiscovery(
        &mdns.MDNSService{},
        nil,
        func(service string, discovered chan *MDNSServiceEntry) error {
            discovered <- expected
            return nil
        },
    )

    entries := make(chan *MDNSServiceEntry, 1)

    require.NoError(t, discovery.Discover(entries))

    require.Len(t, entries, 1)
    assert.Same(t, expected, <-entries)
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

    err := discovery.Discover(make(chan *MDNSServiceEntry))

    require.ErrorIs(t, err, lookupErr)
}

func TestMDNSDiscoveryDiscoverFiltersEntriesByService(t *testing.T) {
    airlinkEntry := &MDNSServiceEntry{
        Name: "SomeDevice._airlink._udp.local.",
    }
    uppercaseAirlinkEntry := &MDNSServiceEntry{
        Name: "AnotherDevice._AIRLINK._UDP.LOCAL.",
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

    entries := make(chan *MDNSServiceEntry, 2)

    require.NoError(t, discovery.Discover(entries))

    assert.Equal(t, 1, lookupCalls)
    require.Equal(t, 2, len(entries))

    assert.Same(t, airlinkEntry, <-entries)
    assert.Same(t, uppercaseAirlinkEntry, <-entries)
}
