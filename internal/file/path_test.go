package file

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveCanonicalPath(t *testing.T) {
	t.Run("resolves path", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.txt")
		require.NoError(t, os.WriteFile(path, []byte("test"), 0644))

		got, err := resolveCanonicalPath(path)

		require.NoError(t, err)
		require.NotNil(t, got)

		expected, err := filepath.EvalSymlinks(path)
		require.NoError(t, err)

		assert.Equal(t, expected, *got)
	})

	t.Run("resolves symlink", func(t *testing.T) {
		dir := t.TempDir()
		target := filepath.Join(dir, "target.txt")
		link := filepath.Join(dir, "link.txt")

		require.NoError(t, os.WriteFile(target, []byte("test"), 0644))
		require.NoError(t, os.Symlink(target, link))

		got, err := resolveCanonicalPath(link)
		require.NoError(t, err)
		require.NotNil(t, got)

		expected, err := filepath.EvalSymlinks(target)
		require.NoError(t, err)

		assert.Equal(t, expected, *got)
	})

	t.Run("returns error for nonexistent path", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing.txt")

		got, err := resolveCanonicalPath(path)

		assert.Nil(t, got)
		assert.Error(t, err)
	})
}
