package node

import (
	"airlink/internal/discovery"
	"fmt"
	"net"

	"github.com/google/uuid"
)

func CreateUDPAddress(ip string, port int) (*net.UDPAddr, error) {
	addr := net.JoinHostPort(ip, fmt.Sprintf("%d", port))
	return net.ResolveUDPAddr("udp", addr)
}

func generateNodeId() (discovery.PeerId, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}

	return discovery.PeerId(id.String()), nil
}
