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
	discovery := newMDNSDiscovery(&mdns.MDNSService{}, func(config *mdns.Config) (mdnsServer, error) {
		newServerCalls++
		assert.NotNil(t, config.Zone)
		return server, nil
	})

	require.NoError(t, discovery.Stop(), "stopping before start must be safe")
	require.NoError(t, discovery.Start())
	require.ErrorIs(t, discovery.Start(), ErrMDNSDiscoveryStarted)
	require.NoError(t, discovery.Stop())
	require.NoError(t, discovery.Stop(), "stopping twice must be safe")

	assert.Equal(t, 1, newServerCalls)
	assert.Equal(t, 1, server.shutdownCalls)
}

func TestMDNSDiscoveryStartRejectsNilService(t *testing.T) {
	discovery := newMDNSDiscovery(nil, func(*mdns.Config) (mdnsServer, error) {
		t.Fatal("server must not be created without a service")
		return nil, nil
	})

	require.ErrorIs(t, discovery.Start(), ErrMDNSServiceNil)
}

func TestMDNSDiscoveryStopCanRetryAfterShutdownFailure(t *testing.T) {
	shutdownErr := errors.New("shutdown failed")
	server := &mockMDNSServer{shutdownErr: shutdownErr}
	discovery := newMDNSDiscovery(&mdns.MDNSService{}, func(*mdns.Config) (mdnsServer, error) {
		return server, nil
	})
	require.NoError(t, discovery.Start())

	require.ErrorIs(t, discovery.Stop(), shutdownErr)
	server.shutdownErr = nil
	require.NoError(t, discovery.Stop())
	assert.Equal(t, 2, server.shutdownCalls)
}
