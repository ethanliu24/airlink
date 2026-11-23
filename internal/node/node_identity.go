package node

import (
	"airlink/internal/metadata"

	"github.com/google/uuid"
)

func generateNodeId() (metadata.PeerId, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}

	return metadata.PeerId(id.String()), nil
}

func (n *P2PNode) NodeID() NodeId {
	return n.nodeId
}
