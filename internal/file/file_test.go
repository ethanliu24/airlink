package file

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateRegularFile(t *testing.T) {
	t.Run("accepts regular file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.txt")
		require.NoError(t, os.WriteFile(path, []byte("test"), 0644))

		file, err := os.Open(path)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = file.Close()
		})

		assert.NoError(t, validateRegularFile(file))
	})

	t.Run("rejects directory", func(t *testing.T) {
		dir := t.TempDir()

		file, err := os.Open(dir)
		require.NoError(t, err)

		err = validateRegularFile(file)

		assert.Error(t, err)
	})

	t.Run("rejects closed file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.txt")
		require.NoError(t, os.WriteFile(path, []byte("test"), 0644))

		file, err := os.Open(path)
		require.NoError(t, err)
		require.NoError(t, file.Close())

		assert.Error(t, validateRegularFile(file))
	})
}
