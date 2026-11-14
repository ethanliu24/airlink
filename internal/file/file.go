package file

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
)

var FileIsNullError = errors.New("file pointer is null - Open() must be called first")

func validateRegularFile(file *os.File) error {
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("stat file: %w", err)
	}

	if !info.Mode().IsRegular() {
		_ = file.Close()
		return fmt.Errorf("path is not a regular file: %s", file.Name())
	}

	return nil
}

func mkdirAllTracked(path string, perm os.FileMode) ([]string, error) {
	path = filepath.Clean(path)

	var missing []string
	for current := path; ; current = filepath.Dir(current) {
		info, err := os.Stat(current)

		if err == nil {
			if !info.IsDir() {
				return nil, fmt.Errorf("path is not a directory: %s", current)
			}
			break
		}

		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}

		missing = append(missing, current)

		parent := filepath.Dir(current)
		if parent == current {
			return nil, fmt.Errorf("could not find existing parent directory")
		}
	}

	var created []string

	for i := range slices.Backward(missing) {
		dir := missing[i]

		err := os.Mkdir(dir, perm)
		if err == nil {
			created = append(created, dir)
			continue
		}

		if errors.Is(err, os.ErrExist) {
			info, statErr := os.Stat(dir)
			if statErr == nil && info.IsDir() {
				continue
			}
		}

		return created, err
	}

	return created, nil
}

func removeCreatedDirs(dirs []string) {
	for i := len(dirs) - 1; i >= 0; i-- {
		_ = os.Remove(dirs[i])
	}
}
