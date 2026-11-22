package node

import "airlink/internal/metadata"

func (n *P2PNode) NodeID() metadata.NodeId {
	return n.nodeId
}
