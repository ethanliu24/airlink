package node

import (
	"airlink/internal/event"
	"airlink/internal/file"
	"airlink/internal/transfer"
)

func (n *P2PNode) RegisterEventHandler(handler event.EventHandlerFunc) error {
	return n.eventManager.RegisterEventHandler(handler)
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
