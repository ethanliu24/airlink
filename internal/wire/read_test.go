package wire

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestReadMessage(t *testing.T) {
	t.Parallel()

	t.Run("reads and deserializes message", func(t *testing.T) {
		t.Parallel()

		expected := wrapperspb.String("hello")

		var buf bytes.Buffer
		require.NoError(t, WriteMessage(&buf, expected))

		actual := &wrapperspb.StringValue{}

		err := ReadMessage(&buf, actual)

		require.NoError(t, err)
		assert.Equal(t, expected.GetValue(), actual.GetValue())
	})

	t.Run("handles message arriving in chunks", func(t *testing.T) {
		t.Parallel()

		expected := wrapperspb.String("hello")

		var buf bytes.Buffer
		require.NoError(t, WriteMessage(&buf, expected))

		reader := &chunkedReader{
			reader:    &buf,
			chunkSize: 1,
		}

		actual := &wrapperspb.StringValue{}

		err := ReadMessage(reader, actual)

		require.NoError(t, err)
		assert.Equal(t, expected.GetValue(), actual.GetValue())
	})

	t.Run("header read error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("read failed")
		reader := &errorReader{err: expectedErr}

		err := ReadMessage(reader, &wrapperspb.StringValue{})

		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("message ends before declared length", func(t *testing.T) {
		t.Parallel()

		data := []byte{
			0, 0, 0, 10,
			1, 2, 3, 4, 5,
		}

		err := ReadMessage(
			bytes.NewReader(data),
			&wrapperspb.StringValue{},
		)

		require.Error(t, err)
		assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})

	t.Run("message is too large", func(t *testing.T) {
		t.Parallel()

		length := uint32(maxMessageLengthBytes + 1)

		header := make([]byte, msgHeaderLengthBytes)
		header[0] = byte(length >> 24)
		header[1] = byte(length >> 16)
		header[2] = byte(length >> 8)
		header[3] = byte(length)

		err := ReadMessage(
			bytes.NewReader(header),
			&wrapperspb.StringValue{},
		)

		require.Error(t, err)
		assert.Equal(t, messageTooLargeError(readOperation, int(length)), err)
	})

	t.Run("invalid protobuf data", func(t *testing.T) {
		t.Parallel()

		data := []byte{0xff, 0xff, 0xff}

		header := make([]byte, msgHeaderLengthBytes)
		header[3] = byte(len(data))

		buf := bytes.NewBuffer(header)
		_, err := buf.Write(data)
		require.NoError(t, err)

		err = ReadMessage(buf, &wrapperspb.StringValue{})

		require.Error(t, err)
	})

	t.Run("msg is nil", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer

		err := ReadMessage(&buf, nil)

		require.Error(t, err)
	})
}
