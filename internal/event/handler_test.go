package event

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandlerLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("register", func(t *testing.T) {
		t.Parallel()

		t.Run("success", func(t *testing.T) {
			h := newHandler()
			var called bool
			callback := func(e Event) {
				called = true
			}

			err := h.register(callback)

			require.NoError(t, err)
			assert.True(t, h.registered)

			mockPayload := &testPayload{id: 1}
			mockEvent, err := NewEvent("test.event", mockPayload)
			require.NoError(t, err)

			h.handleEvent(mockEvent)
			assert.True(t, called)
		})

		t.Run("fails when handler already registered", func(t *testing.T) {
			h := newHandler()
			callback1 := func(e Event) {}
			callback2 := func(e Event) {}

			err1 := h.register(callback1)
			require.NoError(t, err1)

			err2 := h.register(callback2)

			require.Error(t, err2)
			assert.Contains(t, err2.Error(), "event handler already registered, cannot replace handler callback")
		})
	})
}
