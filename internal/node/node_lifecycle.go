package node

func (n *P2PNode) Start() error {
	n.eventManager.StartListening()

	err := n.listen()
	if err != nil {
		return err
	}

	err = n.GoOnline()
	if err != nil {
		n.receiver.CleanUp()
		return err
	}

	return nil
}

func (n *P2PNode) Cleanup() {
	// TODO log errors or smth
	n.peerManager.Stop()
	n.eventManager.CleanUp()
	n.transferManager.CleanUp()
	n.receiver.CleanUp()
	n.sender.CleanUp()
	n.transport.Close()
}
