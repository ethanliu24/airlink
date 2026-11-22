package node

import (
	"airlink/internal/event"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type nodeTestEventPayload struct {
	Message string `json:"message"`
}

func TestRegisterEventHandler(t *testing.T) {
	t.Parallel()

	t.Run("receives emitted event", func(t *testing.T) {
		t.Parallel()

		node := newStartedTestNode(t)

		received := make(chan event.Event, 1)

		err := node.RegisterEventHandler(func(e event.Event) error {
			received <- e
			return nil
		})

		require.NoError(t, err)

		expectedEvent, err := event.NewEvent(
			"node.test",
			&nodeTestEventPayload{
				Message: "hello",
			},
		)
		require.NoError(t, err)

		err = node.eventManager.EmitEvent(expectedEvent)
		require.NoError(t, err)

		select {
		case got := <-received:
			assert.Equal(t, expectedEvent.Name(), got.Name())

			payload, ok := got.Payload().(*nodeTestEventPayload)
			require.True(t, ok)

			assert.Equal(t, "hello", payload.Message)

		case <-time.After(nodeTestTimeout):
			t.Fatal("timed out waiting for registered event handler")
		}
	})
}
