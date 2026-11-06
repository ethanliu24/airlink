package transfer

import (
	"airlink/internal/event"
	"airlink/internal/file"
	"fmt"
	"io"
	"net"
	"sync"
)

const idGenRetryTimes = 3

type Manager struct {
	transfers map[string]transfer
	mu        sync.Mutex

	addr       net.Addr
	openReader file.OpenReaderFunc
	emitEvent  event.EmitEventFunc
	generateID generateIdTransferFunc
}

func (m *Manager) Send(stream io.ReadWriteCloser, filename string) (TransferId, error) {
	reader, err := m.openReader(filename)
	if err != nil {
		_ = stream.Close()
		return "", err
	}

	transfer := newSendTransfer(reader, stream, m.addr)
	id, err := m.register(transfer)

	if err != nil {
		_ = reader.Close()
		_ = stream.Close()
		return "", err
	}

	go func() {
		transfer.run()
	}()

	return id, nil
}

func (m *Manager) Receive(stream io.ReadWriteCloser) (TransferId, error) {
	transfer := newReceiveTransfer(stream, m.emitEvent)
	id, err := m.register(transfer)

	if err != nil {
		_ = stream.Close()
		return "", err
	}

	go func() {
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

func (m *Manager) GetTransfer(id TransferId) (transfer, bool) {
	transfer, ok := m.transfers[id]
	return transfer, ok
}

func (m *Manager) SubmitTransferAcceptDecision(id TransferId, accepted bool) error {
	receiveTransfer, exist := m.GetTransfer(id)
	if !exist {
		return fmt.Errorf("SubmitTransferAcceptDecision receive transfer does not exist - id: %s", id)
	}

	transfer, ok := receiveTransfer.(*receive)
	if !ok {
		return fmt.Errorf("SubmitTransferAcceptDecision transfer is not a receive transfer - id: %s", id)
	}

	transfer.userTransferAcceptanceDecision(accepted)
	return nil
}

func NewManager(addr net.Addr, openReader file.OpenReaderFunc, emitEvent event.EmitEventFunc) *Manager {
	return &Manager{
		transfers:  make(map[string]transfer),
		addr:       addr,
		openReader: openReader,
		emitEvent:  emitEvent,
		generateID: generateTransferId,
	}
}
