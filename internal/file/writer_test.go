package file

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileWriterWrite(t *testing.T) {
	t.Run("writes data successfully", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.txt")

		f, err := os.Create(path)
		require.NoError(t, err)

		writer := &FileWriter{
			file: f,
		}

		data := []byte("hello world")

		n, err := writer.Write(data)

		require.NoError(t, err)
		assert.Equal(t, len(data), n)

		require.NoError(t, writer.Close())

		got, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, data, got)
	})

	t.Run("returns error when file is nil", func(t *testing.T) {
		writer := &FileWriter{}

		n, err := writer.Write([]byte("hello"))

		require.ErrorIs(t, err, FileIsNullError)
		assert.Zero(t, n)
	})

	t.Run("returns error when file is closed", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.txt")

		f, err := os.Create(path)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		writer := &FileWriter{
			file: f,
		}

		n, err := writer.Write([]byte("hello"))

		require.ErrorIs(t, err, os.ErrClosed)
		assert.Zero(t, n)
	})
}

func TestFileWriterClose(t *testing.T) {
	t.Run("closes file successfully", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.txt")

		f, err := os.Create(path)
		require.NoError(t, err)

		writer := &FileWriter{
			file: f,
		}

		err = writer.Close()

		require.NoError(t, err)

		_, err = writer.Write([]byte("hello"))
		assert.ErrorIs(t, err, os.ErrClosed)
	})

	t.Run("returns error when file is nil", func(t *testing.T) {
		writer := &FileWriter{}

		err := writer.Close()

		require.ErrorIs(t, err, FileIsNullError)
	})
}

func TestOpenWriterIntegration(t *testing.T) {
	t.Run("returns usable FileWriter", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "nested", "test.txt")

		writer, err := OpenWriter(path)

		require.NoError(t, err)
		require.NotNil(t, writer)

		fileWriter, ok := writer.(*FileWriter)
		require.True(t, ok)
		require.NotNil(t, fileWriter.file)

		data := []byte("hello world")

		n, err := writer.Write(data)
		require.NoError(t, err)
		assert.Equal(t, len(data), n)

		require.NoError(t, writer.Close())

		got, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, data, got)
	})

	t.Run("returns duplicate error for existing file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.txt")
		original := []byte("existing contents")

		require.NoError(t, os.WriteFile(path, original, 0644))

		writer, err := OpenWriter(path)

		require.ErrorIs(t, err, DuplicateFileFound)
		assert.Nil(t, writer)

		got, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, original, got)
	})

	t.Run("returns error when destination is a directory", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test")
		require.NoError(t, os.Mkdir(path, 0755))

		writer, err := OpenWriter(path)

		require.Error(t, err)
		assert.Nil(t, writer)
		assert.DirExists(t, path)
	})
}

func TestFileWriterCommit(t *testing.T) {
	t.Run("placeholder returns nil", func(t *testing.T) {
		writer := &FileWriter{}

		assert.NoError(t, writer.Commit())
	})
}

func TestFileWriterAbort(t *testing.T) {
	t.Run("placeholder returns nil", func(t *testing.T) {
		writer := &FileWriter{}

		assert.NoError(t, writer.Abort())
	})
}
