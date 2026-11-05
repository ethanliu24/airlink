
package event

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test with this too: go test -race ./internal/event -count=10

const managerTestTimeout = 2 * time.Second

type testEventPayload struct {
	Value string
}

func newTestEvent(t *testing.T, name string) Event {
	t.Helper()

	event, err := NewEvent(name, &testEventPayload{
		Value: "test",
	})
	require.NoError(t, err)

	return event
}

func TestNewManager(t *testing.T) {
	t.Parallel()

	manager := NewManager(func(Event) {})

	require.NotNil(t, manager)
	require.NotNil(t, manager.bus)
	require.NotNil(t, manager.handler)
	require.NotNil(t, manager.done)

	assert.NotNil(t, manager.bus.Events())
}

func TestEmitEvent(t *testing.T) {
	t.Parallel()

	handled := make(chan Event, 1)

	manager := NewManager(func(event Event) {
		handled <- event
	})

	manager.StartListening()
	defer manager.CleanUp()

	expectedEvent := newTestEvent(t, "test.event")

	err := manager.EmitEvent(expectedEvent)
	require.NoError(t, err)

	select {
	case got := <-handled:
		assert.Equal(t, expectedEvent.Name(), got.Name())
		assert.Equal(t, expectedEvent.Payload(), got.Payload())

	case <-time.After(managerTestTimeout):
		t.Fatal("timed out waiting for event")
	}
}

func TestStartListening(t *testing.T) {
	t.Parallel()

	firstEvent := newTestEvent(t, "first.event")
	secondEvent := newTestEvent(t, "second.event")

	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseFirst := make(chan struct{})

	var releaseOnce sync.Once

	release := func() {
		releaseOnce.Do(func() {
			close(releaseFirst)
		})
	}

	manager := NewManager(func(event Event) {
		switch event.Name() {
		case "first.event":
			close(firstStarted)
			<-releaseFirst

		case "second.event":
			close(secondStarted)
		}
	})

	defer func() {
		release()
		manager.CleanUp()
	}()

	// Starting multiple times must not create multiple listeners.
	manager.StartListening()
	manager.StartListening()
	manager.StartListening()

	require.NoError(t, manager.EmitEvent(firstEvent))

	select {
	case <-firstStarted:
	case <-time.After(managerTestTimeout):
		t.Fatal("first event handler did not start")
	}

	// Publish another event while the first handler is blocked.
	secondEmitDone := make(chan error, 1)

	go func() {
		secondEmitDone <- manager.EmitEvent(secondEvent)
	}()

	// The second handler must not run concurrently with the first.
	select {
	case <-secondStarted:
		t.Fatal("second event was handled concurrently")
	case <-time.After(100 * time.Millisecond):
	}

	release()

	select {
	case err := <-secondEmitDone:
		require.NoError(t, err)
	case <-time.After(managerTestTimeout):
		t.Fatal("second event could not be emitted")
	}

	select {
	case <-secondStarted:
	case <-time.After(managerTestTimeout):
		t.Fatal("second event handler did not start")
	}
}

func TestManagerCleanup(t *testing.T) {
	t.Parallel()

	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})

	manager := NewManager(func(Event) {
		close(handlerStarted)
		<-releaseHandler
	})

	manager.StartListening()

	require.NoError(t, manager.EmitEvent(newTestEvent(t, "test.event")))

	select {
	case <-handlerStarted:
	case <-time.After(managerTestTimeout):
		t.Fatal("event handler did not start")
	}

	cleanupDone := make(chan struct{})

	go func() {
		manager.CleanUp()
		close(cleanupDone)
	}()

	// Cleanup must wait for the active handler.
	select {
	case <-cleanupDone:
		t.Fatal("cleanup returned before handler finished")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseHandler)

	select {
	case <-cleanupDone:
	case <-time.After(managerTestTimeout):
		t.Fatal("cleanup did not finish")
	}

	// Cleanup should be safe to call more than once.
	assert.NotPanics(t, func() {
		manager.CleanUp()
	})
}

func TestManagerCleanupBeforeStart(t *testing.T) {
	t.Parallel()

	manager := NewManager(func(Event) {})

	assert.NotPanics(t, func() {
		manager.CleanUp()
	})

	// Starting after cleanup must not start another listener.
	manager.StartListening()

	select {
	case <-manager.done:
	case <-time.After(managerTestTimeout):
		t.Fatal("listener did not terminate")
	}

	err := manager.EmitEvent(newTestEvent(t, "test.event"))
	assert.ErrorIs(t, err, ErrManagerClosed)
}

func TestEmitEventAfterCleanup(t *testing.T) {
	t.Parallel()

	manager := NewManager(func(Event) {})

	manager.StartListening()
	manager.CleanUp()

	err := manager.EmitEvent(newTestEvent(t, "test.event"))

	assert.ErrorIs(t, err, ErrManagerClosed)
}

func TestConcurrentEmitEventAndCleanup(t *testing.T) {
	t.Parallel()

	const publisherCount = 100

	var (
		handledCount  atomic.Int32
		acceptedCount atomic.Int32
	)

	manager := NewManager(func(Event) {
		handledCount.Add(1)
	})

	manager.StartListening()

	event := newTestEvent(t, "test.concurrent")

	var publishers sync.WaitGroup
	publishers.Add(publisherCount)

	start := make(chan struct{})

	for range publisherCount {
		go func() {
			defer publishers.Done()

			<-start

			err := manager.EmitEvent(event)

			switch {
			case err == nil:
				acceptedCount.Add(1)

			case errors.Is(err, ErrManagerClosed):
				// Cleanup won the race.
				// Rejecting this event is expected.

			default:
				t.Errorf("unexpected EmitEvent error: %v", err)
			}
		}()
	}

	cleanupDone := make(chan struct{})

	go func() {
		<-start
		manager.CleanUp()
		close(cleanupDone)
	}()

	// Start publishers and cleanup concurrently.
	close(start)

	publishers.Wait()

	select {
	case <-cleanupDone:
	case <-time.After(managerTestTimeout):
		t.Fatal("cleanup did not finish")
	}

	// Every accepted event must have been handled.
	assert.Equal(
		t,
		acceptedCount.Load(),
		handledCount.Load(),
	)
}
