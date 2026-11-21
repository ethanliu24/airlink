package node

import (
	"airlink/internal/discovery"
	"airlink/internal/event"
	"airlink/internal/file"
	"airlink/internal/transfer"
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"os"
	"strings"

	"github.com/quic-go/quic-go"
)

const IP_ADDRESS = "127.0.0.1"

type P2PNode struct {
	nodeId          discovery.PeerId
	addr            net.Addr
	transport       *quic.Transport
	tlsConfig       *tls.Config
	quicConfig      *quic.Config
	receiver        *receiver
	sender          *sender
	eventManager    *event.Manager
	transferManager *transfer.Manager
	mdnsDiscovery   discovery.Discovery
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

	err = n.mdnsDiscovery.Start()
	if err != nil {
		n.receiver.Cleanup()
		return err
	}

	return nil
}

func (n *P2PNode) NodeID() discovery.PeerId {
	return n.nodeId
}

func (n *P2PNode) isSelf(peer discovery.Peer) bool {
    return peer.Id != "" && peer.Id == n.nodeId
}

func (n *P2PNode) DiscoverPeers() ([]discovery.Peer, error) {
	discoveredPeers, err := n.mdnsDiscovery.Discover()

	peers := make([]discovery.Peer, 0, len(discoveredPeers))
	for _, peer := range discoveredPeers {
		if n.isSelf(peer) {
			continue
		}

		peers = append(peers, peer)
	}

	return peers, err
}

func (n *P2PNode) SendFile(receiverAddr *net.UDPAddr, filename string) error {
	return n.sender.Send(receiverAddr, filename, n.tlsConfig, n.quicConfig)
}

func (n *P2PNode) RegisterEventHandler(handler event.EventHandlerFunc) error {
	return n.eventManager.RegisterEventHandler(handler)
}

func (n *P2PNode) Cleanup() {
	// TODO log errors or smth
	n.mdnsDiscovery.Stop()
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
	discoverMDNS discovery.DiscoveryMDNSFunc,
) (*P2PNode, error) {
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

	tlsConfig := generateTLSConfig()
	quicConfig := getQuicConfig()
	ctx, cancel := context.WithCancel(context.Background())

	transport := &quic.Transport{Conn: udpConn}
	eventManager := event.NewManager()
	transferManager := transfer.NewManager(boundAddr, openReader, eventManager.EmitEvent, ctx, cancel)
	receiver := newReceiver(transport, transferManager.Receive)
	sender := newSender(transport, boundAddr, transferManager.Send)

	return &P2PNode{
		nodeId:          id,
		addr:            boundAddr,
		tlsConfig:       tlsConfig,
		quicConfig:      quicConfig,
		transport:       transport,
		receiver:        receiver,
		sender:          sender,
		eventManager:    eventManager,
		transferManager: transferManager,
		mdnsDiscovery:   mdnsDiscovery,
	}, nil
}
