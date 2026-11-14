package file

import (
	"errors"
	"fmt"
	"os"
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

