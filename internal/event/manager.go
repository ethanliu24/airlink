package event

type Manager struct {
	bus     *bus
	handler *handler
}

func NewManager(eventHandler EventHandlerFunc) *Manager {
	bus := newBus()
	handler := newHandler()
	handler.register(eventHandler)

	return &Manager{
		bus:     bus,
		handler: handler,
	}
}
