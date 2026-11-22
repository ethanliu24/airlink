package node

import (
	"airlink/internal/discovery"
	"airlink/internal/event"
	"airlink/internal/file"
	"airlink/internal/metadata"
	"airlink/internal/security"
	"airlink/internal/transfer"
	"airlink/internal/transport"
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/quic-go/quic-go"
)

type P2PNode struct {
	nodeId     metadata.NodeId
	addr       net.Addr
	identity   *security.Identity
	clientTLS  *tls.Config
	serverTLS  *tls.Config
	quicConfig *quic.Config

	transport *quic.Transport
	receiver  transport.Receiver
	sender    transport.Sender

	eventManager    *event.Manager
	transferManager *transfer.Manager
	peerManager     *discovery.PeerManager

	cancelPeerDiscovery context.CancelFunc
	discoveryMu         sync.Mutex
}

type udpListenFunc func(network string, address *net.UDPAddr) (*net.UDPConn, error)

func CreateUDPAddress(ip string, port int) (*net.UDPAddr, error) {
	addr := net.JoinHostPort(ip, fmt.Sprintf("%d", port))
	return net.ResolveUDPAddr("udp", addr)
}

func generateNodeId() (metadata.PeerId, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}

	return metadata.PeerId(id.String()), nil
}

func getQuicConfig() *quic.Config {
	return &quic.Config{
		// InitialStreamReceiveWindow: 1<<20, // 1 MB
		// MaxStreamReceiveWindow: 6<<20, // 6 MB
		// InitialConnectionReceiveWindow: 2<<20, // 2 MB
		// MaxConnectionReceiveWindow: 12<<20, // 12 MB
		// MaxIncomingStreams: 100, // bidirectional streams
		// MaxIncomingUniStreams: 100, // unidirectional streams
		MaxIdleTimeout:  30 * time.Second,
		KeepAlivePeriod: 15 * time.Second,
	}
}

func NewP2PNode(addr *net.UDPAddr) (*P2PNode, error) {
	return newP2PNode(addr, net.ListenUDP, file.OpenReader, newMDNSDiscovery)
}

func newMDNSDiscovery(addr *net.UDPAddr, id metadata.PeerId) (discovery.Discovery, error) {
	hostName, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	displayName := strings.TrimSuffix(hostName, ".")
	displayName = strings.TrimSuffix(displayName, ".local")

	uniqueInstanceName := "Airlink-" + string(id)

	info := []string{
		discovery.MDNSPeerIDTXTKey + "=" + string(id),
		discovery.MDNSPeerNameTXTKey + "=" + displayName,
		discovery.MDNSProtocolVersionTXTKey + "=" + discovery.AirlinkProtocolVersion,
	}

	return discovery.NewMDNSDiscovery(
		uniqueInstanceName,
		discovery.MDNSServiceName,
		discovery.MDNSDomainName,
		"", // Let the library determine the host FQDN.
		addr.Port,
		[]net.IP{addr.IP}, // Advertise the actual listening IP.
		info,
	)
}

func newP2PNode(
	addr *net.UDPAddr,
	listen udpListenFunc,
	openReader file.OpenReaderFunc,
	discoverMDNS discovery.DiscoverMDNSFunc,
) (*P2PNode, error) {
	identity, err := security.GenerateIdentity()
	if err != nil {
		return nil, err
	}

	// TODO store the id after, use the already generated id, and fallback to generation only if no id
	id, err := generateNodeId()
	if err != nil {
		slog.Error("could not create node id", "err", err)
		return nil, err
	}

	udpConn, err := listen(addr.Network(), addr)
	if err != nil {
		slog.Error("could not listen on UDP address", "address", addr, "err", err)
		return nil, err
	}

	boundAddr := addr
	if localAddr, ok := udpConn.LocalAddr().(*net.UDPAddr); ok {
		boundAddr = localAddr
	}

	mdnsDiscovery, err := discoverMDNS(boundAddr, id)
	if err != nil {
		return nil, err
	}

	clientTLS := identity.TLSConfig()
	clientTLS.InsecureSkipVerify = true

	serverTLS := identity.TLSConfig()
	serverTLS.ClientAuth = tls.RequireAnyClientCert

	quicConfig := getQuicConfig()
	ctx, cancel := context.WithCancel(context.Background())

	eventManager := event.NewManager()
	quicTransport := &quic.Transport{Conn: udpConn}
	transferManager := transfer.NewManager(boundAddr, openReader, eventManager.EmitEvent, ctx, cancel)
	receiver := transport.NewReceiver(quicTransport, transferManager.Receive)
	sender := transport.NewSender(quicTransport, boundAddr, transferManager.Send)

	peerManager, err := discovery.NewPeerManager(id, mdnsDiscovery)
	if err != nil {
		udpConn.Close()
		return nil, err
	}

	return &P2PNode{
		nodeId:     id,
		addr:       boundAddr,
		identity:   identity,
		clientTLS:  clientTLS,
		serverTLS:  serverTLS,
		quicConfig: quicConfig,

		transport: quicTransport,
		receiver:  receiver,
		sender:    sender,

		eventManager:    eventManager,
		transferManager: transferManager,
		peerManager:     peerManager,
	}, nil
}
