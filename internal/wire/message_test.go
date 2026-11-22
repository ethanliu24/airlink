package wire

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestMessageRoundTrip(t *testing.T) {
	t.Parallel()

	expected := wrapperspb.String("hello world")

	var buf bytes.Buffer

	require.NoError(t, WriteMessage(&buf, expected))

	actual := &wrapperspb.StringValue{}

	require.NoError(t, ReadMessage(&buf, actual))

	assert.Equal(t, expected.GetValue(), actual.GetValue())
}
