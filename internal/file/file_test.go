
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
		t.Cleanup(func() {
			_ = file.Close()
		})

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

func TestMkdirAllTracked(t *testing.T) {
	t.Run("creates nested directories and tracks them", func(t *testing.T) {
		root := t.TempDir()
		existing := filepath.Join(root, "existing")
		require.NoError(t, os.Mkdir(existing, 0755))

		target := filepath.Join(existing, "one", "two")

		created, err := mkdirAllTracked(target, 0755)

		require.NoError(t, err)
		assert.Equal(t, []string{
			filepath.Join(existing, "one"),
			filepath.Join(existing, "one", "two"),
		}, created)

		assert.DirExists(t, target)
		assert.DirExists(t, existing)
	})

	t.Run("does not track existing directories", func(t *testing.T) {
		existing := t.TempDir()

		created, err := mkdirAllTracked(existing, 0755)

		require.NoError(t, err)
		assert.Empty(t, created)
		assert.DirExists(t, existing)
	})

	t.Run("rejects file in directory path", func(t *testing.T) {
		root := t.TempDir()
		filePath := filepath.Join(root, "file.txt")

		require.NoError(t, os.WriteFile(filePath, []byte("test"), 0644))

		created, err := mkdirAllTracked(
			filepath.Join(filePath, "nested"),
			0755,
		)

		require.Error(t, err)
		assert.Empty(t, created)
		assert.FileExists(t, filePath)
	})

	t.Run("tracks directories for rollback", func(t *testing.T) {
		root := t.TempDir()
		existing := filepath.Join(root, "existing")
		require.NoError(t, os.Mkdir(existing, 0755))

		target := filepath.Join(existing, "one", "two")

		created, err := mkdirAllTracked(target, 0755)
		require.NoError(t, err)

		removeCreatedDirs(created)

		assert.NoDirExists(t, target)
		assert.NoDirExists(t, filepath.Join(existing, "one"))
		assert.DirExists(t, existing)
	})
}

func TestRemoveCreatedDirs(t *testing.T) {
	t.Run("removes created directories in reverse order", func(t *testing.T) {
		root := t.TempDir()
		parent := filepath.Join(root, "parent")
		child := filepath.Join(parent, "child")

		require.NoError(t, os.Mkdir(parent, 0755))
		require.NoError(t, os.Mkdir(child, 0755))

		removeCreatedDirs([]string{parent, child})

		assert.NoDirExists(t, parent)
		assert.NoDirExists(t, child)
		assert.DirExists(t, root)
	})

	t.Run("does not remove non-empty directories", func(t *testing.T) {
		root := t.TempDir()
		parent := filepath.Join(root, "parent")
		child := filepath.Join(parent, "child")
		filePath := filepath.Join(child, "test.txt")

		require.NoError(t, os.MkdirAll(child, 0755))
		require.NoError(t, os.WriteFile(filePath, []byte("test"), 0644))

		removeCreatedDirs([]string{parent, child})

		assert.DirExists(t, parent)
		assert.DirExists(t, child)
		assert.FileExists(t, filePath)
	})

	t.Run("handles empty directory list", func(t *testing.T) {
		assert.NotPanics(t, func() {
			removeCreatedDirs(nil)
		})
	})
}

func TestOpenWriter(t *testing.T) {
	t.Run("creates file and missing parent directories", func(t *testing.T) {
		root := t.TempDir()
		path := filepath.Join(root, "nested", "deeper", "hello.txt")
		data := []byte("hello world")

		writer, err := OpenWriter(path)

		require.NoError(t, err)
		require.NotNil(t, writer)

		n, err := writer.Write(data)
		require.NoError(t, err)
		assert.Equal(t, len(data), n)

		require.NoError(t, writer.Close())

		got, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, data, got)

		assert.DirExists(t, filepath.Dir(path))
	})

	t.Run("rejects duplicate file", func(t *testing.T) {
		root := t.TempDir()
		path := filepath.Join(root, "hello.txt")
		original := []byte("existing contents")

		require.NoError(t, os.WriteFile(path, original, 0644))

		writer, err := OpenWriter(path)

		require.ErrorIs(t, err, DuplicateFileFound)
		assert.Nil(t, writer)

		got, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, original, got)
	})

	t.Run("rejects directory as target", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "target")
		require.NoError(t, os.Mkdir(path, 0755))

		writer, err := OpenWriter(path)

		require.Error(t, err)
		assert.Nil(t, writer)
		assert.DirExists(t, path)
	})

	t.Run("does not remove existing parent directories on failure", func(t *testing.T) {
		root := t.TempDir()
		parent := filepath.Join(root, "existing")
		path := filepath.Join(parent, "hello.txt")

		require.NoError(t, os.Mkdir(parent, 0755))
		require.NoError(t, os.WriteFile(path, []byte("existing"), 0644))

		writer, err := OpenWriter(path)

		require.ErrorIs(t, err, DuplicateFileFound)
		assert.Nil(t, writer)
		assert.DirExists(t, parent)
		assert.FileExists(t, path)
	})
}
