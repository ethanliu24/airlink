package event

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testPayload struct {
	id int
}

func TestEventPayloadExtensibility(t *testing.T) {
	t.Parallel()

	t.Run("allows pointer payloads", func(t *testing.T) {
		id := 123
		payload := &testPayload{id: id}
		event := &event{
			name: "test",
			payload: payload,
		}

		require.Equal(t, event.Name(), "test")

		p, ok := event.Payload().(*testPayload)

		require.True(t, ok)
		require.NotNil(t, p)
		assert.Equal(t, p.id, id)
	})

	t.Run("allows primitive data payloads", func(t *testing.T) {
		payload := 123
		event := &event{
			name: "test",
			payload: payload,
		}

		require.Equal(t, event.Name(), "test")

		p, ok := event.payload.(int)

		require.True(t, ok)
		require.NotNil(t, p)
		assert.Equal(t, p, 123)
	})
}

func TestNewEvent(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		eventName := "user.created"
		payload := &testPayload{id: 42}

		event, err := NewEvent(eventName, payload)

		require.NoError(t, err)
		require.NotNil(t, event)
		assert.Equal(t, eventName, event.Name())
		assert.Equal(t, payload, event.Payload())
	})

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		t.Run("fails when payload is passed by value", func(t *testing.T) {
			t.Parallel()

			event, err := NewEvent("user.created", testPayload{id: 42})

			require.Error(t, err)
			assert.Nil(t, event)
			assert.Contains(t, err.Error(), "must be a pointer, got struct")
		})

		t.Run("fails when payload is a primitive pointer", func(t *testing.T) {
			t.Parallel()

			event, err := NewEvent("user.created", new(int))

			require.Error(t, err)
			assert.Nil(t, event)
			assert.Contains(t, err.Error(), "must point to a struct, got pointer to int")
		})

		t.Run("fails when payload is nil", func(t *testing.T) {
			t.Parallel()

			event, err := NewEvent("user.created", nil)

			require.Error(t, err)
			assert.Nil(t, event)
			assert.Contains(t, err.Error(), "must be a pointer, got invalid")
		})
	})
}
