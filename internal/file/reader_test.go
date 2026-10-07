package file

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenReader(t *testing.T) {
	t.Run("opens regular file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.txt")
		require.NoError(t, os.WriteFile(path, []byte("hello"), 0644))

		reader, err := OpenReader(path)

		require.NoError(t, err)
		require.NotNil(t, reader)

		t.Cleanup(func() {
			_ = reader.Close()
		})
	})

	t.Run("opens file through symlink", func(t *testing.T) {
		if os.Getenv("GOOS") == "windows" {
			t.Skip("symlink creation may require elevated privileges on Windows")
		}

		dir := t.TempDir()
		target := filepath.Join(dir, "target.txt")
		link := filepath.Join(dir, "link.txt")

		require.NoError(t, os.WriteFile(target, []byte("hello"), 0644))
		require.NoError(t, os.Symlink(target, link))

		reader, err := OpenReader(link)

		require.NoError(t, err)
		require.NotNil(t, reader)

		t.Cleanup(func() {
			_ = reader.Close()
		})

		data, err := io.ReadAll(reader)
		require.NoError(t, err)

		assert.Equal(t, []byte("hello"), data)
	})

	t.Run("returns error for nonexistent file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "does-not-exist.txt")

		reader, err := OpenReader(path)

		assert.Nil(t, reader)
		assert.Error(t, err)
	})

	t.Run("returns error for broken symlink", func(t *testing.T) {
		if os.Getenv("GOOS") == "windows" {
			t.Skip("symlink creation may require elevated privileges on Windows")
		}

		dir := t.TempDir()
		target := filepath.Join(dir, "does-not-exist.txt")
		link := filepath.Join(dir, "broken-link.txt")

		require.NoError(t, os.Symlink(target, link))

		reader, err := OpenReader(link)

		assert.Nil(t, reader)
		assert.Error(t, err)
	})

	t.Run("rejects directory", func(t *testing.T) {
		path := t.TempDir()

		reader, err := OpenReader(path)

		assert.Nil(t, reader)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "path is not a regular file")
	})
}

func TestFileReader_Read(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.txt")
	content := []byte("hello, world")

	require.NoError(t, os.WriteFile(path, content, 0644))

	reader, err := OpenReader(path)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = reader.Close()
	})

	buf := make([]byte, len(content))

	n, err := reader.Read(buf)

	require.NoError(t, err)
	assert.Equal(t, len(content), n)
	assert.Equal(t, content, buf[:n])
}

func TestFileReader_ReadMultipleChunks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.txt")
	content := []byte("abcdefghijklmnopqrstuvwxyz")

	require.NoError(t, os.WriteFile(path, content, 0644))

	reader, err := OpenReader(path)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = reader.Close()
	})

	buf := make([]byte, 4)
	var result []byte

	for {
		n, err := reader.Read(buf)

		if n > 0 {
			result = append(result, buf[:n]...)
		}

		if errors.Is(err, io.EOF) {
			break
		}

		require.NoError(t, err)
	}

	assert.Equal(t, content, result)
}

func TestFileReader_Close(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.txt")

	require.NoError(t, os.WriteFile(path, []byte("hello"), 0644))

	reader, err := OpenReader(path)
	require.NoError(t, err)

	require.NoError(t, reader.Close())

	_, err = reader.Read(make([]byte, 1))
	assert.Error(t, err)
}

func TestFileReader_CloseNilFile(t *testing.T) {
	reader := &FileReader{}

	err := reader.Close()

	assert.ErrorIs(t, err, FileIsNullError)
}
