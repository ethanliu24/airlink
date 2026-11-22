package wire

import (
	"encoding/binary"
	"io"

	"google.golang.org/protobuf/proto"
)

func WriteMessage(w io.Writer, msg proto.Message) error {
	if msg == nil {
		return messageIsNilError(writeOperation)
	}

	// serialize
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	if len(data) > maxMessageLengthBytes {
		return messageTooLargeError(writeOperation, len(data))
	}

	// header
	length := uint32(len(data))
	header := make([]byte, msgHeaderLengthBytes)
	binary.BigEndian.PutUint32(header, length)

	// write
	err = writeFull(w, header)
	if err != nil {
		return err
	}

	return writeFull(w, data)
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
