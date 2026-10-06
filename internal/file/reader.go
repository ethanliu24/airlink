package file

import (
	"errors"
	"io"
	"os"
)

var FileIsNullError = errors.New("file pointer is null - Open() must be called first")

const BYTES_TO_READ int = 1024 * 4

type Reader interface {
	io.ReadCloser
}

type FileReader struct {
	file *os.File
}

func (fr *FileReader) Read(p []byte) (int, error) {
	return fr.file.Read(p)
}

func (fr *FileReader) Close() error {
	if fr.file == nil {
		return FileIsNullError
	}

	return fr.file.Close()
}

type OpenReaderFunc func(path string) (Reader, error)

func OpenReader(path string) (Reader, error) {
	canonicalPath, err := resolveCanonicalPath(path)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(*canonicalPath)
	if err != nil {
		return nil, err
	}

	err = validateRegularFile(file)
	if err != nil {
		return nil, err
	}

	return &FileReader{
		file: file,
	}, nil
}
