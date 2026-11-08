package transfer

import (
	"airlink/internal/event"
	"airlink/internal/file"
	"context"
	"fmt"
	"net"
	"sync"
)

const idGenRetryTimes = 3

type Manager struct {
	transfers map[string]transfer
	mu        sync.Mutex
	ctx       context.Context
	cancel    context.CancelFunc

	addr       net.Addr
	openReader file.OpenReaderFunc
	emitEvent  event.EmitEventFunc
	generateID generateIdTransferFunc
}

func (m *Manager) Send(stream TransferStream, filename string) (TransferId, error) {
	reader, err := m.openReader(filename)
	if err != nil {
		_ = stream.Close()
		return "", err
	}

	ctx, cancel := context.WithCancel(m.ctx)
	transfer := newSendTransfer(ctx, reader, stream, m.addr, transferAcceptTimeoutDuration)
	id, err := m.register(transfer)

	if err != nil {
		cancel()
		_ = reader.Close()
		_ = stream.Close()
		return "", err
	}

	go func() {
		defer cancel()
		transfer.run()
	}()

	return id, nil
}

func (m *Manager) Receive(stream TransferStream) (TransferId, error) {
	ctx, cancel := context.WithCancel(m.ctx)
	transfer := newReceiveTransfer(stream, m.emitEvent, ctx, cancel, transferAcceptTimeoutDuration)
	id, err := m.register(transfer)

	if err != nil {
		cancel()
		_ = stream.Close()
		return "", err
	}

	go func() {
		defer cancel()
		transfer.run()
	}()

	return id, nil
}

func (m *Manager) register(t transfer) (TransferId, error) {
	for range idGenRetryTimes {
		id, err := m.generateID()
		if err != nil {
			return "", err
		}

		m.mu.Lock()
		_, used := m.transfers[id]
		if !used {
			t.setId(id)
			m.transfers[id] = t
			m.mu.Unlock()
			return id, nil
		}

		m.mu.Unlock()
	}

	return "", fmt.Errorf("manager receive transfer id generation max retries reached - impossible!")
}

func (m *Manager) getTransfer(id TransferId) (transfer, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	transfer, ok := m.transfers[id]
	return transfer, ok
}

func (m *Manager) getReceiveTransfer(id TransferId, callsite string) (*receive, error) {
	receiveTransfer, exist := m.getTransfer(id)
	if !exist {
		return nil, fmt.Errorf("%s receive transfer does not exist - id: %s", callsite, id)
	}

	transfer, ok := receiveTransfer.(*receive)
	if !ok {
		return nil, fmt.Errorf("%s transfer is not a receive transfer - id: %s", callsite, id)
	}

	return transfer, nil
}

func (m *Manager) SubmitTransferAcceptDecision(id TransferId, accepted bool) error {
	transfer, err := m.getReceiveTransfer(id, "SubmitTransferAcceptDecision")
	if err != nil {
		return err
	}

	transfer.userTransferAcceptanceDecision(accepted)
	return nil
}

func (m *Manager) SubmitDownloadDestination(id TransferId, destination string) error {
	transfer, err := m.getReceiveTransfer(id, "SubmitDownloadDestination")
	if err != nil {
		return err
	}

	return transfer.userDownloadDestinationDecision(destination)
}

func (m *Manager) CleanUp() {
	m.cancel()
}

func NewManager(
	addr net.Addr,
	openReader file.OpenReaderFunc,
	emitEvent event.EmitEventFunc,
	ctx context.Context,
	cancel context.CancelFunc,
) *Manager {
	return &Manager{
		transfers:  make(map[string]transfer),
		ctx:        ctx,
		cancel:     cancel,
		addr:       addr,
		openReader: openReader,
		emitEvent:  emitEvent,
		generateID: generateTransferId,
	}
}
