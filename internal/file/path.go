package file

import (
	"fmt"
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
