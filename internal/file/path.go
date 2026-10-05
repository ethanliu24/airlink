package file

import (
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
