package wire

import "fmt"

// Send protobuf schema data in format [ 4 byte header, length of data ][ data ]
const (
	msgHeaderLengthBytes  = 4
	maxMessageLengthBytes = 8 * 1024
	writeOperation        = "transfer protocol write"
	readOperation         = "transfer protocol read"
)

func messageIsNilError(operation string) error {
	return fmt.Errorf("%s: message is nil", operation)
}

func messageTooLargeError(operation string, length int) error {
	return fmt.Errorf("message too large in %s: %d bytes", operation, length)
}
