
package file

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileWriterWrite(t *testing.T) {
	t.Run("writes data successfully", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "staging.tmp")

		f, err := os.Create(path)
		require.NoError(t, err)

		writer := &FileWriter{
			stagingFile: f,
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

	t.Run("returns error when staging file is nil", func(t *testing.T) {
		writer := &FileWriter{}

		n, err := writer.Write([]byte("hello"))

		require.ErrorIs(t, err, FileIsNullError)
		assert.Zero(t, n)
	})

	t.Run("returns error when staging file is closed", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "staging.tmp")

		f, err := os.Create(path)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		writer := &FileWriter{
			stagingFile: f,
		}

		n, err := writer.Write([]byte("hello"))

		require.ErrorIs(t, err, os.ErrClosed)
		assert.Zero(t, n)
	})
}

func TestFileWriterClose(t *testing.T) {
	t.Run("closes staging file successfully", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "staging.tmp")

		f, err := os.Create(path)
		require.NoError(t, err)

		writer := &FileWriter{
			stagingFile: f,
		}

		err = writer.Close()

		require.NoError(t, err)

		_, err = writer.Write([]byte("hello"))
		assert.ErrorIs(t, err, os.ErrClosed)
	})

	t.Run("returns error when staging file is nil", func(t *testing.T) {
		writer := &FileWriter{}

		err := writer.Close()

		require.ErrorIs(t, err, FileIsNullError)
	})
}

func TestFileWriterCommit(t *testing.T) {
	t.Run("publishes staging file when destination does not exist", func(t *testing.T) {
		destination := filepath.Join(t.TempDir(), "hello.txt")

		writer, err := OpenWriter(destination)
		require.NoError(t, err)

		fileWriter := writer.(*FileWriter)
		stagingPath := fileWriter.stagingPath
		data := []byte("hello world")

		n, err := writer.Write(data)
		require.NoError(t, err)
		assert.Equal(t, len(data), n)

		require.NoError(t, writer.Close())
		require.NoError(t, writer.Commit(WriteCommitRaise))

		got, err := os.ReadFile(destination)
		require.NoError(t, err)
		assert.Equal(t, data, got)

		assert.NoFileExists(t, stagingPath)
	})

	t.Run("returns duplicate error when destination already exists", func(t *testing.T) {
		root := t.TempDir()
		destination := filepath.Join(root, "hello.txt")
		original := []byte("existing contents")

		require.NoError(t, os.WriteFile(destination, original, 0644))

		writer, err := OpenWriter(destination)
		require.NoError(t, err)

		fileWriter := writer.(*FileWriter)
		stagingPath := fileWriter.stagingPath
		data := []byte("new contents")

		n, err := writer.Write(data)
		require.NoError(t, err)
		assert.Equal(t, len(data), n)

		require.NoError(t, writer.Close())

		err = writer.Commit(WriteCommitRaise)
		require.ErrorIs(t, err, DuplicateFileFound)

		got, err := os.ReadFile(destination)
		require.NoError(t, err)
		assert.Equal(t, original, got)

		stagedData, err := os.ReadFile(stagingPath)
		require.NoError(t, err)
		assert.Equal(t, data, stagedData)
	})

	t.Run("replaces existing destination when requested", func(t *testing.T) {
		root := t.TempDir()
		destination := filepath.Join(root, "hello.txt")
		original := []byte("existing contents")

		require.NoError(t, os.WriteFile(destination, original, 0644))

		writer, err := OpenWriter(destination)
		require.NoError(t, err)

		fileWriter := writer.(*FileWriter)
		stagingPath := fileWriter.stagingPath
		data := []byte("new contents")

		n, err := writer.Write(data)
		require.NoError(t, err)
		assert.Equal(t, len(data), n)

		require.NoError(t, writer.Close())
		require.NoError(t, writer.Commit(WriteCommitReplace))

		got, err := os.ReadFile(destination)
		require.NoError(t, err)
		assert.Equal(t, data, got)

		assert.NoFileExists(t, stagingPath)
	})

	t.Run("returns error when commit mode is unrecognized", func(t *testing.T) {
		destination := filepath.Join(t.TempDir(), "hello.txt")

		writer, err := OpenWriter(destination)
		require.NoError(t, err)

		fileWriter := writer.(*FileWriter)
		stagingPath := fileWriter.stagingPath

		_, err = writer.Write([]byte("hello world"))
		require.NoError(t, err)
		require.NoError(t, writer.Close())

		err = writer.Commit(WriterCommitMode("invalid"))
		require.Error(t, err)

		assert.NoFileExists(t, destination)
		assert.FileExists(t, stagingPath)
	})
}

func TestFileWriterAbort(t *testing.T) {
	t.Run("closes and removes staging file", func(t *testing.T) {
		root := t.TempDir()
		destination := filepath.Join(root, "nested", "deeper", "hello.txt")

		writer, err := OpenWriter(destination)
		require.NoError(t, err)

		fileWriter := writer.(*FileWriter)
		stagingPath := fileWriter.stagingPath

		_, err = writer.Write([]byte("hello world"))
		require.NoError(t, err)

		require.FileExists(t, stagingPath)

		require.NoError(t, writer.Abort())

		assert.NoFileExists(t, stagingPath)
		assert.NoFileExists(t, destination)
		assert.NoDirExists(t, filepath.Join(root, "nested"))

		// The staging file handle should also be closed.
		_, err = writer.Write([]byte("more data"))
		assert.ErrorIs(t, err, os.ErrClosed)
	})

	t.Run("does not remove existing destination", func(t *testing.T) {
		root := t.TempDir()
		destination := filepath.Join(root, "hello.txt")
		original := []byte("existing contents")

		require.NoError(t, os.WriteFile(destination, original, 0644))

		writer, err := OpenWriter(destination)
		require.NoError(t, err)

		fileWriter := writer.(*FileWriter)
		stagingPath := fileWriter.stagingPath

		_, err = writer.Write([]byte("new contents"))
		require.NoError(t, err)

		require.NoError(t, writer.Close())
		require.NoError(t, writer.Abort())

		assert.NoFileExists(t, stagingPath)

		got, err := os.ReadFile(destination)
		require.NoError(t, err)
		assert.Equal(t, original, got)
	})

	t.Run("can abort repeatedly", func(t *testing.T) {
		destination := filepath.Join(t.TempDir(), "hello.txt")

		writer, err := OpenWriter(destination)
		require.NoError(t, err)

		fileWriter := writer.(*FileWriter)
		stagingPath := fileWriter.stagingPath

		_, err = writer.Write([]byte("hello world"))
		require.NoError(t, err)

		require.NoError(t, writer.Abort())
		require.NoError(t, writer.Abort())

		assert.NoFileExists(t, stagingPath)
		assert.NoFileExists(t, destination)
	})
}

func TestGenerateStagingCanonicalPath(t *testing.T) {
	t.Run("generates staging path in destination directory", func(t *testing.T) {
		destination := filepath.Join(t.TempDir(), "hello.txt")

		stagingPath, err := generateStagingCanonicalPath(destination)

		require.NoError(t, err)

		assert.Equal(t, filepath.Dir(destination), filepath.Dir(stagingPath))
		assert.NotEqual(t, destination, stagingPath)
		assert.True(t, strings.HasPrefix(
			filepath.Base(stagingPath),
			"hello-staging-",
		))
	})

	t.Run("generates unique paths", func(t *testing.T) {
		destination := filepath.Join(t.TempDir(), "hello.txt")

		first, err := generateStagingCanonicalPath(destination)
		require.NoError(t, err)

		second, err := generateStagingCanonicalPath(destination)
		require.NoError(t, err)

		assert.NotEqual(t, first, second)
	})

	t.Run("handles destination without extension", func(t *testing.T) {
		destination := filepath.Join(t.TempDir(), "README")

		stagingPath, err := generateStagingCanonicalPath(destination)

		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(
			filepath.Base(stagingPath),
			"README-staging-",
		))
	})
}

func TestOpenWriter(t *testing.T) {
	t.Run("creates staging file and missing parent directories", func(t *testing.T) {
		root := t.TempDir()
		destination := filepath.Join(root, "nested", "deeper", "hello.txt")

		writer, err := OpenWriter(destination)

		require.NoError(t, err)
		require.NotNil(t, writer)

		fileWriter, ok := writer.(*FileWriter)
		require.True(t, ok)
		require.NotNil(t, fileWriter.stagingFile)

		stagingPath := fileWriter.stagingFile.Name()
		expectedDir, err := filepath.EvalSymlinks(filepath.Dir(destination))

		require.NoError(t, err)
		assert.Equal(t, expectedDir, filepath.Dir(writer.(*FileWriter).stagingFile.Name()))
		assert.NotEqual(t, destination, stagingPath)
		assert.FileExists(t, stagingPath)
		assert.NoFileExists(t, destination)

		data := []byte("hello world")

		n, err := writer.Write(data)
		require.NoError(t, err)
		assert.Equal(t, len(data), n)

		require.NoError(t, writer.Close())

		got, err := os.ReadFile(stagingPath)
		require.NoError(t, err)
		assert.Equal(t, data, got)

		assert.NoFileExists(t, destination)
		assert.DirExists(t, filepath.Dir(destination))

		require.NoError(t, os.Remove(stagingPath))
	})

	t.Run("creates staging file when destination already exists", func(t *testing.T) {
		root := t.TempDir()
		destination := filepath.Join(root, "hello.txt")
		original := []byte("existing contents")

		require.NoError(t, os.WriteFile(destination, original, 0644))

		writer, err := OpenWriter(destination)

		require.NoError(t, err)
		require.NotNil(t, writer)

		fileWriter, ok := writer.(*FileWriter)
		require.True(t, ok)

		stagingPath := fileWriter.stagingFile.Name()

		assert.NotEqual(t, destination, stagingPath)
		assert.FileExists(t, stagingPath)

		data := []byte("new contents")

		n, err := writer.Write(data)
		require.NoError(t, err)
		assert.Equal(t, len(data), n)

		require.NoError(t, writer.Close())

		got, err := os.ReadFile(destination)
		require.NoError(t, err)
		assert.Equal(t, original, got)

		stagedData, err := os.ReadFile(stagingPath)
		require.NoError(t, err)
		assert.Equal(t, data, stagedData)

		require.NoError(t, os.Remove(stagingPath))
	})

	t.Run("returns error when destination is a directory", func(t *testing.T) {
		destination := filepath.Join(t.TempDir(), "target")
		require.NoError(t, os.Mkdir(destination, 0755))

		writer, err := OpenWriter(destination)

		require.Error(t, err)
		assert.Nil(t, writer)
		assert.DirExists(t, destination)
	})

	t.Run("returns error when parent path is a file", func(t *testing.T) {
		root := t.TempDir()
		parent := filepath.Join(root, "file.txt")

		require.NoError(t, os.WriteFile(parent, []byte("test"), 0644))

		destination := filepath.Join(parent, "nested", "hello.txt")

		writer, err := OpenWriter(destination)

		require.Error(t, err)
		assert.Nil(t, writer)
		assert.FileExists(t, parent)
	})
}
