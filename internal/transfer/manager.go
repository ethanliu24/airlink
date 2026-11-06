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

func NewManager(addr net.Addr, openReader file.OpenReaderFunc, emitEvent event.EmitEventFunc) *Manager {
	return &Manager{
		transfers:  make(map[string]transfer),
		addr:       addr,
		openReader: openReader,
		emitEvent:  emitEvent,
		generateID: generateTransferId,
	}
}
