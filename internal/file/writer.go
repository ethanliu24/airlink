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

var DuplicateFileFound = errors.New("duplicate file found")

type Writer interface {
	io.WriteCloser
	Commit() error
	Abort() error
}

type FileWriter struct {
    destination string
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

func (fw *FileWriter) Commit() error {
	return nil
}

func (fw *FileWriter) Abort() error {
	return nil
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

// TODO write to a staging file in tmp/ before actually saving it
// TODO add modes for dup handling: ask (default), replace/override, rename, cancel

func OpenWriter(path string) (Writer, error) {
	canonicalPath, err := resolveCanonicalPathForCreation(path)
	if err != nil {
		return nil, err
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

	stagingFile, err := os.OpenFile(
		stagingCanonicalPath,
		os.O_WRONLY | os.O_CREATE | os.O_EXCL,
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
		stagingFile: stagingFile,
	}, nil
}
