package wire

import (
	"encoding/binary"
	"io"

	"google.golang.org/protobuf/proto"
)

func ReadMessage(r io.Reader, msg proto.Message) error {
	if msg == nil {
		return messageIsNilError(readOperation)
	}

	// header
	header := make([]byte, msgHeaderLengthBytes)

	_, err := io.ReadFull(r, header)
	if err != nil {
		return err
	}

	// parse header
	length := binary.BigEndian.Uint32(header)
	if length > maxMessageLengthBytes {
		return messageTooLargeError(readOperation, int(length))
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
