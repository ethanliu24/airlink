package event

type bus struct {
	queue chan Event
}

func (b *bus) addEvent(event Event) {
	b.queue <- event
}

func (b *bus) Events() chan Event {
	return b.queue
}

func (b *bus) cleanUp() {
	close(b.queue)
}

// TODO Add size cap for safety, need to modify addEvent with select statement for non blocking
func newBus() *bus {
	return &bus{
		queue: make(chan Event),
	}
}
