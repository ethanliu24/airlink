package transfer

import (
	"encoding/binary"
	"io"

	"google.golang.org/protobuf/proto"
)

// Send field data in format [ 4 byte header, length of data ][ data ]
const MSG_HEADER_LENGTH_BYTES = 4

func writeMessage(w io.Writer, msg proto.Message) error {
	// serialize
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	// header
	length := uint32(len(data))
	header := make([]byte, MSG_HEADER_LENGTH_BYTES)
	binary.BigEndian.PutUint32(header, length)

	// write
	_, err = w.Write(header)
	if err != nil {
		return err
	}

	_, err = w.Write(data)
	if err != nil {
		return err
	}

	return nil
}
