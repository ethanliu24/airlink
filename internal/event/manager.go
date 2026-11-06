package event

import (
	"errors"
	"sync"
)

type Manager struct {
	bus     *bus
	handler *handler

	startOnce sync.Once
	stopOnce  sync.Once
	done      chan struct{}

	mu     sync.RWMutex
	closed bool
}

var ErrManagerClosed = errors.New("event manager is closed")

type EmitEventFunc func(Event) error

func (m *Manager) EmitEvent(event Event) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return ErrManagerClosed
	}

	m.bus.addEvent(event)
	return nil
}

func (m *Manager) RegisterEventHandler(handler EventHandlerFunc) error {
	return m.handler.register(handler)
}

func (m *Manager) listen() {
	defer close(m.done)

	for event := range m.bus.Events() {
		// Should not hand this to a go routine for sequential event handling and avoid unbounded events
		m.handler.handleEvent(event)
	}

}

func (m *Manager) StartListening() {
	m.startOnce.Do(func() {
		go m.listen()
	})
}

func (m *Manager) CleanUp() {
	// avoids deadlock in the case of initializing a manager then calling clean up right away
	// StartListening's logic will only execute once
	m.StartListening()

	m.stopOnce.Do(func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		m.closed = true
		m.bus.cleanUp()
	})

	<-m.done
}

func NewManager() *Manager {
	bus := newBus()
	handler := newHandler()

	return &Manager{
		bus:     bus,
		handler: handler,
		done:    make(chan struct{}),
		closed:  false,
	}
}
