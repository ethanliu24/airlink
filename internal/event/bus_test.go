package event

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBusLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("successfully adds and gets events sequentially using a goroutine", func(t *testing.T) {
		t.Parallel()

		b := newBus()
		mockPayload := &testPayload{id: 99}
		mockEvent, err := NewEvent("test.event", mockPayload)
		require.NoError(t, err)

		go func() {
			b.addEvent(mockEvent)
		}()

		receivedEvent := <-b.Events()
		require.NotNil(t, receivedEvent)
		assert.Equal(t, mockEvent.Name(), receivedEvent.Name())
		assert.Equal(t, mockEvent.Payload(), receivedEvent.Payload())
	})

	t.Run("successfully closes the queue on cleanUp", func(t *testing.T) {
		t.Parallel()

		b := newBus()
		b.cleanUp()

		done := make(chan struct{})
		var received Event

		go func() {
			received = <-b.Events()
			close(done)
		}()

		select {
		case <-done:
			assert.Nil(t, received)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("getEvent blocked indefinitely after cleanUp was called")
		}
	})

	t.Run("panics when trying to add an event after cleanUp", func(t *testing.T) {
		t.Parallel()

		b := newBus()
		b.cleanUp()

		mockPayload := &testPayload{id: 100}
		mockEvent, err := NewEvent("test.panic", mockPayload)
		require.NoError(t, err)

		assert.Panics(t, func() {
			b.addEvent(mockEvent)
		})
	})
}
