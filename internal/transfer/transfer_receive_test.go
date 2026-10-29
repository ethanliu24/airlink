package transfer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransferReceiveRun(t *testing.T) {
	t.Parallel()

	t.Run("advances from init to request ingestion", func(t *testing.T) {
		t.Parallel()

		receiver := &transferReceive{
			state: receiveInit,
		}

		result := receiver.run()

		require.NotNil(t, result)
		require.NoError(t, result.Err)

		assert.Equal(t, receiveIngestingRequest, receiver.state)
		assert.True(t, receiver.consumed)
	})

	t.Run("rejects second invocation", func(t *testing.T) {
		t.Parallel()

		receiver := &transferReceive{
			state: receiveInit,
		}

		firstResult := receiver.run()
		require.NoError(t, firstResult.Err)

		secondResult := receiver.run()

		require.NotNil(t, secondResult)
		require.ErrorContains(t, secondResult.Err, "already called")
		assert.Equal(t, receiveError, receiver.state)
	})

	t.Run("stops when already done", func(t *testing.T) {
		t.Parallel()

		receiver := &transferReceive{
			state: receiveDone,
		}

		result := receiver.run()

		require.NotNil(t, result)
		require.NoError(t, result.Err)
		assert.Equal(t, receiveDone, receiver.state)
		assert.True(t, receiver.consumed)
	})

	t.Run("stops when already in error state", func(t *testing.T) {
		t.Parallel()

		receiver := &transferReceive{
			state: receiveError,
		}

		result := receiver.run()

		require.NotNil(t, result)
		require.NoError(t, result.Err)
		assert.Equal(t, receiveError, receiver.state)
	})
}
