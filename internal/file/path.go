package file

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func resolveCanonicalPath(path string) (*string, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve path: %w", err)
	}

	canonicalPath, err := filepath.EvalSymlinks(absPath)
	if err != nil {
		return nil, fmt.Errorf("resolve symlinks: %w", err)
	}

	return &canonicalPath, nil
}

func resolveCanonicalPathForCreation(path string) (string, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}

	absPath = filepath.Clean(absPath)

	var missing []string
	current := absPath

	for {
		_, err := os.Lstat(current)

		if err == nil {
			canonicalPath, err := filepath.EvalSymlinks(current)
			if err != nil {
				return "", fmt.Errorf("resolve symlinks: %w", err)
			}

			if len(missing) > 0 {
				info, err := os.Stat(canonicalPath)
				if err != nil {
					return "", fmt.Errorf("stat existing parent: %w", err)
				}

				if !info.IsDir() {
					return "", fmt.Errorf("parent path is not a directory: %s", current)
				}
			}

			for i := len(missing) - 1; i >= 0; i-- {
				canonicalPath = filepath.Join(canonicalPath, missing[i])
			}

			return filepath.Clean(canonicalPath), nil
		}

		if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("resolve symlinks: %w", err)
		}

		parent := filepath.Dir(current)
		if parent == current {
			return "", fmt.Errorf("could not find existing parent directory")
		}

		missing = append(missing, filepath.Base(current))
		current = parent
	}
}
