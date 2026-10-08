package transfer

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

type errorWriter struct {
	err error
}

func (w *errorWriter) Write([]byte) (int, error) {
	return 0, w.err
}

type failAfterNWriter struct {
	n   int
	err error
}

func (w *failAfterNWriter) Write(p []byte) (int, error) {
	if w.n == 0 {
		return 0, w.err
	}

	n := min(len(p), w.n)
	w.n -= n

	if n < len(p) {
		return n, w.err
	}

	return n, nil
}

type errorReader struct {
	err error
}

func (r *errorReader) Read([]byte) (int, error) {
	return 0, r.err
}

type chunkedReader struct {
	reader    io.Reader
	chunkSize int
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	if len(p) > r.chunkSize {
		p = p[:r.chunkSize]
	}

	return r.reader.Read(p)
}

func TestWriteMessage(t *testing.T) {
	t.Parallel()

	t.Run("writes header and serialized message", func(t *testing.T) {
		t.Parallel()

		msg := wrapperspb.String("hello")
		var buf bytes.Buffer

		err := writeMessage(&buf, msg)

		require.NoError(t, err)

		data := buf.Bytes()
		require.GreaterOrEqual(t, len(data), MSG_HEADER_LENGTH_BYTES)

		length := int(data[0])<<24 |
			int(data[1])<<16 |
			int(data[2])<<8 |
			int(data[3])

		expectedData, err := proto.Marshal(msg)
		require.NoError(t, err)

		assert.Equal(t, len(expectedData), length)
		assert.Equal(t, expectedData, data[MSG_HEADER_LENGTH_BYTES:])
	})

	t.Run("message is too large", func(t *testing.T) {
		t.Parallel()

		msg := wrapperspb.String(string(make([]byte, MAX_MESSAGE_LENGTH_BYTES)))

		err := writeMessage(io.Discard, msg)

		require.Error(t, err)
		assert.Equal(
			t,
			messageTooLargeError(MAX_MESSAGE_LENGTH_BYTES+3),
			err,
		)
	})

	t.Run("header write error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("header write failed")

		writer := &errorWriter{err: expectedErr}

		err := writeMessage(writer, wrapperspb.String("hello"))

		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("data write error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("data write failed")

		writer := &failAfterNWriter{
			n:   MSG_HEADER_LENGTH_BYTES,
			err: expectedErr,
		}

		err := writeMessage(writer, wrapperspb.String("hello"))

		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
	})
}

func TestReadMessage(t *testing.T) {
	t.Parallel()

	t.Run("reads and deserializes message", func(t *testing.T) {
		t.Parallel()

		expected := wrapperspb.String("hello")

		var buf bytes.Buffer
		require.NoError(t, writeMessage(&buf, expected))

		actual := &wrapperspb.StringValue{}

		err := readMessage(&buf, actual)

		require.NoError(t, err)
		assert.Equal(t, expected.GetValue(), actual.GetValue())
	})

	t.Run("handles message arriving in chunks", func(t *testing.T) {
		t.Parallel()

		expected := wrapperspb.String("hello")

		var buf bytes.Buffer
		require.NoError(t, writeMessage(&buf, expected))

		reader := &chunkedReader{
			reader:    &buf,
			chunkSize: 1,
		}

		actual := &wrapperspb.StringValue{}

		err := readMessage(reader, actual)

		require.NoError(t, err)
		assert.Equal(t, expected.GetValue(), actual.GetValue())
	})

	t.Run("header read error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("read failed")
		reader := &errorReader{err: expectedErr}

		err := readMessage(reader, &wrapperspb.StringValue{})

		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("message ends before declared length", func(t *testing.T) {
		t.Parallel()

		data := []byte{
			0, 0, 0, 10,
			1, 2, 3, 4, 5,
		}

		err := readMessage(
			bytes.NewReader(data),
			&wrapperspb.StringValue{},
		)

		require.Error(t, err)
		assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})

	t.Run("message is too large", func(t *testing.T) {
		t.Parallel()

		length := uint32(MAX_MESSAGE_LENGTH_BYTES + 1)

		header := make([]byte, MSG_HEADER_LENGTH_BYTES)
		header[0] = byte(length >> 24)
		header[1] = byte(length >> 16)
		header[2] = byte(length >> 8)
		header[3] = byte(length)

		err := readMessage(
			bytes.NewReader(header),
			&wrapperspb.StringValue{},
		)

		require.Error(t, err)
		assert.Equal(t, messageTooLargeError(int(length)), err)
	})

	t.Run("invalid protobuf data", func(t *testing.T) {
		t.Parallel()

		data := []byte{0xff, 0xff, 0xff}

		header := make([]byte, MSG_HEADER_LENGTH_BYTES)
		header[3] = byte(len(data))

		buf := bytes.NewBuffer(header)
		_, err := buf.Write(data)
		require.NoError(t, err)

		err = readMessage(buf, &wrapperspb.StringValue{})

		require.Error(t, err)
	})
}

func TestMessageRoundTrip(t *testing.T) {
	t.Parallel()

	expected := wrapperspb.String("hello world")

	var buf bytes.Buffer

	require.NoError(t, writeMessage(&buf, expected))

	actual := &wrapperspb.StringValue{}

	require.NoError(t, readMessage(&buf, actual))

	assert.Equal(t, expected.GetValue(), actual.GetValue())
}
