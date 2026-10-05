package file

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenReader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")

	content := []byte("hello, world")

	err := os.WriteFile(path, content, 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	reader, err := OpenReader(path)
	if err != nil {
		t.Fatalf("OpenReader() returned error: %v", err)
	}
	defer reader.Close()

	if reader.file == nil {
		t.Fatal("OpenReader() returned reader with nil file")
	}
}

func TestOpenReader_FileDoesNotExist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist.txt")

	_, err := OpenReader(path)
	if err == nil {
		t.Fatal("OpenReader() expected error for nonexistent file")
	}
}

func TestFileReader_Read(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")

	content := []byte("hello, world")

	err := os.WriteFile(path, content, 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	reader, err := OpenReader(path)
	if err != nil {
		t.Fatalf("OpenReader() returned error: %v", err)
	}
	defer reader.Close()

	buf := make([]byte, BYTES_TO_READ)

	n, err := reader.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("Read() returned unexpected error: %v", err)
	}

	if got := string(buf[:n]); got != string(content) {
		t.Fatalf("Read() = %q, want %q", got, content)
	}
}

func TestFileReader_ReadMultipleChunks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")

	content := []byte("abcdefghijklmnopqrstuvwxyz")

	err := os.WriteFile(path, content, 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	reader, err := OpenReader(path)
	if err != nil {
		t.Fatalf("OpenReader() returned error: %v", err)
	}
	defer reader.Close()

	buf := make([]byte, 4)
	var got []byte

	for {
		n, err := reader.Read(buf)

		if n > 0 {
			got = append(got, buf[:n]...)
		}

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			t.Fatalf("Read() returned unexpected error: %v", err)
		}
	}

	if string(got) != string(content) {
		t.Fatalf("Read() = %q, want %q", got, content)
	}
}

func TestFileReader_Close(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")

	err := os.WriteFile(path, []byte("hello"), 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	reader, err := OpenReader(path)
	if err != nil {
		t.Fatalf("OpenReader() returned error: %v", err)
	}

	if err := reader.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}
}

func TestFileReader_CloseWithoutFile(t *testing.T) {
	reader := &FileReader{}

	err := reader.Close()

	if !errors.Is(err, FileIsNullError) {
		t.Fatalf("Close() = %v, want %v", err, FileIsNullError)
	}
}
