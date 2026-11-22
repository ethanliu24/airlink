package wire

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestWriteMessage(t *testing.T) {
	t.Parallel()

	t.Run("writes header and serialized message", func(t *testing.T) {
		t.Parallel()

		msg := wrapperspb.String("hello")
		var buf bytes.Buffer

		err := WriteMessage(&buf, msg)

		require.NoError(t, err)

		data := buf.Bytes()
		require.GreaterOrEqual(t, len(data), msgHeaderLengthBytes)

		length := int(data[0])<<24 |
			int(data[1])<<16 |
			int(data[2])<<8 |
			int(data[3])

		expectedData, err := proto.Marshal(msg)
		require.NoError(t, err)

		assert.Equal(t, len(expectedData), length)
		assert.Equal(t, expectedData, data[msgHeaderLengthBytes:])
	})

	t.Run("message is too large", func(t *testing.T) {
		t.Parallel()

		msg := wrapperspb.String(string(make([]byte, maxMessageLengthBytes)))

		err := WriteMessage(io.Discard, msg)

		require.Error(t, err)
		assert.Equal(
			t,
			messageTooLargeError(writeOperation, maxMessageLengthBytes+3),
			err,
		)
	})

	t.Run("header write error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("header write failed")

		writer := &errorWriter{err: expectedErr}

		err := WriteMessage(writer, wrapperspb.String("hello"))

		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("data write error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("data write failed")

		writer := &failAfterNWriter{
			n:   msgHeaderLengthBytes,
			err: expectedErr,
		}

		err := WriteMessage(writer, wrapperspb.String("hello"))

		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("msg is nil", func(t *testing.T) {
		t.Parallel()

		err := WriteMessage(io.Discard, nil)

		require.Error(t, err)
	})
}
