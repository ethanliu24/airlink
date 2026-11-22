package node

import (
	"airlink/internal/discovery"
	"airlink/internal/event"
	"airlink/internal/file"
	"airlink/internal/security"
	"airlink/internal/transfer"
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

type P2PNode struct {
	nodeId     discovery.PeerId
	identity   *security.Identity
	addr       net.Addr
	tlsConfig *tls.Config
	quicConfig *quic.Config

	transport *quic.Transport
	receiver  *receiver
	sender    *sender

	eventManager    *event.Manager
	transferManager *transfer.Manager
	peerManager     *discovery.PeerManager

	cancelPeerDiscovery context.CancelFunc
	discoveryMu         sync.Mutex
}

func (n *P2PNode) listen() error {
	return n.receiver.Listen(n.tlsConfig, n.quicConfig)
}

func (n *P2PNode) Start() error {
	n.eventManager.StartListening()

	err := n.listen()
	if err != nil {
		return err
	}

	err = n.GoOnline()
	if err != nil {
		n.receiver.Cleanup()
		return err
	}

	return nil
}

func (n *P2PNode) NodeID() discovery.PeerId {
	return n.nodeId
}

func (n *P2PNode) GoOnline() error {
	return n.peerManager.Start()
}

func (n *P2PNode) GoOffline() error {
	return n.peerManager.Stop()
}

func (n *P2PNode) GetPeers() []discovery.PeerRecord {
	return n.peerManager.GetPeers()
}

func (n *P2PNode) StartPeerDiscovery(pollingFrequency time.Duration) {
	n.discoveryMu.Lock()

	if n.cancelPeerDiscovery != nil {
		n.cancelPeerDiscovery()
	}

	ctx, cancel := context.WithCancel(context.Background())
	n.cancelPeerDiscovery = cancel

	n.discoveryMu.Unlock()

	go func() {
		ticker := time.NewTicker(pollingFrequency)
		defer ticker.Stop()

	outer:
		for {
			select {
			case <-ticker.C:
				n.peerManager.Discover() // TODO if err then log
			case <-ctx.Done():
				break outer
			}
		}
	}()
}

func (n *P2PNode) StopPeerDiscovery() {
	n.discoveryMu.Lock()
	defer n.discoveryMu.Unlock()

	if n.cancelPeerDiscovery != nil {
		n.cancelPeerDiscovery()
		n.cancelPeerDiscovery = nil
	}
}

func (n *P2PNode) SendFile(receiverAddr *net.UDPAddr, filename string) error {
	return n.sender.Send(receiverAddr, filename, n.tlsConfig, n.quicConfig)
}

func (n *P2PNode) RegisterEventHandler(handler event.EventHandlerFunc) error {
	return n.eventManager.RegisterEventHandler(handler)
}

func (n *P2PNode) Cleanup() {
	// TODO log errors or smth
	n.peerManager.Stop()
	n.eventManager.CleanUp()
	n.transferManager.CleanUp()
	n.receiver.Cleanup()
	n.sender.Cleanup()
	n.transport.Close()
}

func (n *P2PNode) SubmitTransferAcceptDecision(id transfer.TransferId, accepted bool) error {
	return n.transferManager.SubmitTransferAcceptDecision(id, accepted)
}

func (n *P2PNode) SubmitDownloadDestination(id transfer.TransferId, destination string) error {
	return n.transferManager.SubmitDownloadDestination(id, destination)
}

func (n *P2PNode) SubmitDuplicatedDestinationHandling(id transfer.TransferId, mode file.WriterCommitMode) error {
	return n.transferManager.SubmitDuplicatedDestinationHandling(id, mode)
}

func NewP2PNode(addr *net.UDPAddr) (*P2PNode, error) {
	return newP2PNode(addr, net.ListenUDP, file.OpenReader, newMDNSDiscovery)
}

func newMDNSDiscovery(addr *net.UDPAddr, id discovery.PeerId) (discovery.Discovery, error) {
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

	udpConn, err := listen(addr.Network(), addr)
	if err != nil {
		slog.Error("could not listen on UDP address", "address", addr, "err", err)
		return nil, err
	}

	boundAddr := addr
	if localAddr, ok := udpConn.LocalAddr().(*net.UDPAddr); ok {
		boundAddr = localAddr
	}

	// TODO store the id after, use the already generated id, and fallback to generation only if no id
	id, err := generateNodeId()
	if err != nil {
		slog.Error("could not create node id", "err", err)
		return nil, err
	}

	mdnsDiscovery, err := discoverMDNS(boundAddr, id)
	if err != nil {
		return nil, err
	}

	tlsConfig := identity.TLSConfig()
	tlsConfig.InsecureSkipVerify = true // TODO temp for local testing
	quicConfig := getQuicConfig()
	ctx, cancel := context.WithCancel(context.Background())

	eventManager := event.NewManager()
	transport := &quic.Transport{Conn: udpConn}
	transferManager := transfer.NewManager(boundAddr, openReader, eventManager.EmitEvent, ctx, cancel)
	receiver := newReceiver(transport, transferManager.Receive)
	sender := newSender(transport, boundAddr, transferManager.Send)

	peerManager, err := discovery.NewPeerManager(id, mdnsDiscovery)
	if err != nil {
		udpConn.Close()
		return nil, err
	}

	return &P2PNode{
		nodeId:     id,
		identity:   identity,
		addr:       boundAddr,
		tlsConfig:  tlsConfig,
		quicConfig: quicConfig,

		transport: transport,
		receiver:  receiver,
		sender:    sender,

		eventManager:    eventManager,
		transferManager: transferManager,
		peerManager:     peerManager,
	}, nil
}
