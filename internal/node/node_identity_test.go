package node

import (
	"airlink/internal/discovery"
	"airlink/internal/metadata"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewP2PNodeGeneratesNodeID(t *testing.T) {
	addr, err := CreateUDPAddress(nodeTestIP, 1234)
	require.NoError(t, err)

	var receivedID metadata.PeerId

	p2pNode, err := newP2PNode(
		addr,
		mockListen,
		mockOpenReader,
		func(_ *net.UDPAddr, nodeID metadata.PeerId) (discovery.Discovery, error) {
			receivedID = nodeID

			return &mockMDNSDiscovery{
				start: func() error { return nil },
				stop:  func() error { return nil },
			}, nil
		},
	)

	require.NoError(t, err)
	t.Cleanup(p2pNode.Cleanup)

	assert.NotEmpty(t, p2pNode.NodeID())
	assert.Equal(t, p2pNode.NodeID(), receivedID)
}

func TestNewP2PNodesHaveDifferentIDs(t *testing.T) {
	addr1, err := CreateUDPAddress(nodeTestIP, 1111)
	require.NoError(t, err)

	addr2, err := CreateUDPAddress(nodeTestIP, 2222)
	require.NoError(t, err)

	node1, err := newP2PNode(addr1, mockListen, mockOpenReader, newNoopMDNSDiscovery)
	require.NoError(t, err)
	t.Cleanup(node1.Cleanup)

	node2, err := newP2PNode(addr2, mockListen, mockOpenReader, newNoopMDNSDiscovery)
	require.NoError(t, err)
	t.Cleanup(node2.Cleanup)

	assert.NotEqual(t, node1.NodeID(), node2.NodeID())
}
