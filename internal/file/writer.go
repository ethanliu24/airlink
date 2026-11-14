package file

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

var DuplicateFileFound = errors.New("duplicate file found")

type Writer interface {
	io.WriteCloser
	Commit() error
	Abort() error
}

type FileWriter struct {
	file      *os.File
	committed bool
}

func (fw *FileWriter) Write(p []byte) (int, error) {
	if fw.file == nil {
		return 0, FileIsNullError
	}

	return fw.file.Write(p)
}

func (fw *FileWriter) Close() error {
	if fw.file == nil {
		return FileIsNullError
	}

	return fw.file.Close()
}

func (fw *FileWriter) Commit() error {
	return nil
}

func (fw *FileWriter) Abort() error {
	return nil
}

type OpenWriterFunc func(path string) error

// TODO write to a staging file in tmp/ before actually saving it
// TODO add modes for dup handling: ask (default), replace/override, rename, cancel

func OpenWriter(path string) (Writer, error) {
    canonicalPath, err := resolveCanonicalPathForCreation(path)
    if err != nil {
        return nil, err
    }

    createdDirs, err := mkdirAllTracked(filepath.Dir(canonicalPath), 0755)
    if err != nil {
        removeCreatedDirs(createdDirs)
        return nil, err
    }

    keepCreatedDirs := false
    defer func() {
        if !keepCreatedDirs {
            removeCreatedDirs(createdDirs)
        }
    }()

    f, err := os.OpenFile(
        canonicalPath,
        os.O_WRONLY|os.O_CREATE|os.O_EXCL,
        0644,
    )

    if err != nil {
        if !errors.Is(err, os.ErrExist) {
            return nil, err
        }

        existing, openErr := os.Open(canonicalPath)
        if openErr != nil {
            return nil, openErr
        }

        validateErr := validateRegularFile(existing)
        closeErr := existing.Close()

        if validateErr != nil {
            return nil, validateErr
        }

        if closeErr != nil {
            return nil, closeErr
        }

        return nil, DuplicateFileFound
    }

    keepCreatedDirs = true

    return &FileWriter{
        file: f,
    }, nil
}
