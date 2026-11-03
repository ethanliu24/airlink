package transfer

import (
	"fmt"
	"sync"
)

const idGenRetryTimes = 3

type Manager struct {
	transfers  map[string]transfer
	mu         sync.Mutex
	generateID generateIdTransferFunc
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
			m.transfers[id] = t
			m.mu.Unlock()
			return id, nil
		}

		m.mu.Unlock()
	}

	return "", fmt.Errorf("manager receive transfer id generation max retries reached - impossible!")
}
