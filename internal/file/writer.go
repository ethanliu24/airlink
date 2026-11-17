package file

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type WriterCommitMode string

const (
	WriteCommitRaise   WriterCommitMode = "raise"
	WriteCommitReplace WriterCommitMode = "replace"
	WriteCommitAbort   WriterCommitMode = "abort"
)

var DuplicateFileFound = errors.New("duplicate file found")

type Writer interface {
	io.WriteCloser
	Commit(WriterCommitMode) error
	Abort() error
}

type FileWriter struct {
	destination string
	stagingPath string
	createdDirs []string
	stagingFile *os.File
	committed   bool
}

func (fw *FileWriter) Write(p []byte) (int, error) {
	if fw.stagingFile == nil {
		return 0, FileIsNullError
	}

	return fw.stagingFile.Write(p)
}

func (fw *FileWriter) Close() error {
	if fw.stagingFile == nil {
		return FileIsNullError
	}

	return fw.stagingFile.Close()
}

func (fw *FileWriter) Commit(m WriterCommitMode) error {
	mode, err := ParseWriteCommitMode(string(m))
	if err != nil {
		return err
	}

	if mode == WriteCommitAbort {
		return fw.Abort()
	}

	commitSuccess := false
	defer func() {
		if !commitSuccess {
			removeCreatedDirs(fw.createdDirs)
		}
	}()

	err = os.Link(fw.stagingPath, fw.destination)

	if err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}

		switch mode {
		case WriteCommitRaise:
			return DuplicateFileFound
		case WriteCommitReplace:
			if err := os.Rename(fw.stagingPath, fw.destination); err != nil {
				return err
			}
		}
	} else {
		if err := os.Remove(fw.stagingPath); err != nil {
			return err
		}
	}

	commitSuccess = true
	return nil
}

func (fw *FileWriter) Abort() error {
	var abortErr error

	if fw.stagingFile != nil {
		if err := fw.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			abortErr = errors.Join(abortErr, err)
		}
	}

	if fw.stagingPath != "" {
		if err := os.Remove(fw.stagingPath); err != nil &&
			!errors.Is(err, os.ErrNotExist) {
			abortErr = errors.Join(abortErr, err)
		}
	}

	removeCreatedDirs(fw.createdDirs)

	return abortErr
}

func generateStagingCanonicalPath(destination string) (string, error) {
	dir := filepath.Dir(destination)
	base := filepath.Base(destination)
	name := strings.TrimSuffix(base, filepath.Ext(base))

	randomBytes := make([]byte, 32)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", err
	}

	r := sha256.Sum256(randomBytes)
	hash := hex.EncodeToString(r[:])
	filename := fmt.Sprintf("%s-staging-%s.tmp", name, hash)
	canonicalPath := filepath.Join(dir, filename)

	return canonicalPath, nil
}

type OpenWriterFunc func(path string) (Writer, error)

func OpenWriter(path string) (Writer, error) {
	canonicalPath, err := resolveCanonicalPathForCreation(path)
	if err != nil {
		return nil, err
	}

	if canonicalPath == "" {
		return nil, fmt.Errorf("writer canonical path is empty")
	}

	err = validateRegularFileStr(canonicalPath)
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

	stagingCanonicalPath, err := generateStagingCanonicalPath(canonicalPath)
	if err != nil {
		return nil, err
	}

	if stagingCanonicalPath == "" {
		return nil, fmt.Errorf("writer staging canonical path is empty")
	}

	stagingFile, err := os.OpenFile(
		stagingCanonicalPath,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0644,
	)

	if err != nil {
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}

		existing, openErr := os.Open(stagingCanonicalPath)
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
		destination: canonicalPath,
		stagingPath: stagingCanonicalPath,
		createdDirs: createdDirs,
		stagingFile: stagingFile,
	}, nil
}

func ParseWriteCommitMode(mode string) (WriterCommitMode, error) {
	m := WriterCommitMode(mode)
	switch m {
	case WriteCommitRaise, WriteCommitReplace, WriteCommitAbort:
		return m, nil
	default:
		return "", fmt.Errorf("unrecognized writer commit mode: %s", mode)
	}
}
