package event

import "fmt"

type EventHandlerFunc func(Event) error

type handler struct {
	handle     EventHandlerFunc
	registered bool
}

func (h *handler) handleEvent(event Event) {
	h.handle(event)
}

func (h *handler) register(callback EventHandlerFunc) error {
	if h.registered {
		return fmt.Errorf("event handler already registered, cannot replace handler callback")
	}

	h.handle = callback
	h.registered = true
	return nil
}

func newHandler() *handler {
	return &handler{
		registered: false,
	}
}
