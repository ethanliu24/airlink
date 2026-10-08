package transfer

import (
	"encoding/binary"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"
)

// Send field data in format [ 4 byte header, length of data ][ data ]
const (
	msgHeaderLengthBytes = 4
	maxMessageLengthBytes = 8 * 1024
)

func messageTooLargeError(length int) error {
	return fmt.Errorf("message too large: %d bytes", length)
}

func writeMessage(w io.Writer, msg proto.Message) error {
	// serialize
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	if len(data) > maxMessageLengthBytes {
		return messageTooLargeError(len(data))
	}

	// header
	length := uint32(len(data))
	header := make([]byte, msgHeaderLengthBytes)
	binary.BigEndian.PutUint32(header, length)

	// write
	_, err = w.Write(header)
	if err != nil {
		return err
	}

	err = writeFull(w, data)
	if err != nil {
		return err
	}

	return writeFull(w, data)
}

func readMessage(r io.Reader, msg proto.Message) error {
	// header
	header := make([]byte, msgHeaderLengthBytes)

	_, err := io.ReadFull(r, header)
	if err != nil {
		return err
	}

	// parse header
	length := binary.BigEndian.Uint32(header)
	if length > maxMessageLengthBytes {
		return messageTooLargeError(int(length))
	}

	// read message
	data := make([]byte, length)

	_, err = io.ReadFull(r, data)
	if err != nil {
		return err
	}

	// deserialize
	if err := proto.Unmarshal(data, msg); err != nil {
		return err
	}

	return nil
}

func writeFull(w io.Writer, data []byte) error {
    for len(data) > 0 {
        n, err := w.Write(data)
        if err != nil {
            return err
        }

        if n == 0 {
            return io.ErrShortWrite
        }

        data = data[n:]
    }

    return nil
}
