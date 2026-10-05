package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	TEST_VALID_IP   = "127.0.0.1"
	TEST_IPV6       = "2001:db8::1"
	TEST_INVALID_IP = "999.999.999.999"
)

func TestCreateUDPAddress(t *testing.T) {
	t.Parallel()

	t.Run("success with ipv4", func(t *testing.T) {
		port := 8080
		addr, err := CreateUDPAddress(TEST_VALID_IP, port)

		require.NoError(t, err)
		require.NotNil(t, addr)
		assert.Equal(t, TEST_VALID_IP, addr.IP.String())
		assert.Equal(t, port, addr.Port)
	})

	t.Run("success with ipv6", func(t *testing.T) {
		port := 9000
		addr, err := CreateUDPAddress(TEST_IPV6, port)

		require.NoError(t, err)
		require.NotNil(t, addr)
		assert.Equal(t, TEST_IPV6, addr.IP.String())
		assert.Equal(t, port, addr.Port)
	})

	t.Run("success with port zero for dynamic allocation", func(t *testing.T) {
		port := 0
		addr, err := CreateUDPAddress(TEST_VALID_IP, port)

		require.NoError(t, err)
		require.NotNil(t, addr)
		assert.Equal(t, TEST_VALID_IP, addr.IP.String())
		assert.Equal(t, port, addr.Port) // The address structure will hold 0 until bound
	})

	t.Run("success with empty ip defaults to wildcard listener", func(t *testing.T) {
		port := 53
		addr, err := CreateUDPAddress("", port)

		require.NoError(t, err)
		require.NotNil(t, addr)
		assert.Nil(t, addr.IP) // Go resolves an empty string host to a nil IP field
		assert.Equal(t, port, addr.Port)
	})

	t.Run("error with invalid ip format", func(t *testing.T) {
		addr, err := CreateUDPAddress(TEST_INVALID_IP, 1234)

		assert.Error(t, err)
		assert.Nil(t, addr)
	})

	t.Run("error with negative port range", func(t *testing.T) {
		addr, err := CreateUDPAddress(TEST_VALID_IP, -1)

		assert.Error(t, err)
		assert.Nil(t, addr)
	})

	t.Run("error with port range overflow", func(t *testing.T) {
		addr, err := CreateUDPAddress(TEST_VALID_IP, 65536)

		assert.Error(t, err)
		assert.Nil(t, addr)
	})
}
