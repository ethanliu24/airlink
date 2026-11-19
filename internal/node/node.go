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

	"github.com/quic-go/quic-go"
)

const IP_ADDRESS = "127.0.0.1"

type P2PNode struct {
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

func (n *P2PNode) DiscoverPeers(entries chan *discovery.MDNSServiceEntry) {
	n.mdnsDiscovery.Discover(entries)
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

func newMDNSDiscovery(addr *net.UDPAddr) (discovery.Discovery, error) {
	hostName, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	return discovery.NewMDNSDiscovery(
		hostName,
		discovery.MDNSServiceName,
		discovery.MDNSDomainName,
		hostName,
		addr.Port,
		nil,
		[]string{},
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

	mdnsDiscovery, err := discoverMDNS(boundAddr)
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
