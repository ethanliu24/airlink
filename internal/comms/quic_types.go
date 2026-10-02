package comms

import "github.com/quic-go/quic-go"


type quicConn struct {
	conn *quic.Conn
}

func (c *quicConn) OpenStream() (Stream, error) {
	stream, err := c.conn.OpenStream()
	if err != nil {
		return nil, err
	}

	return &quicStream{stream: stream}, nil
}

func (c *quicConn) CloseWithError(
	code quic.ApplicationErrorCode,
	msg string,
) error {
	return c.conn.CloseWithError(code, msg)
}

type quicStream struct {
	stream *quic.Stream
}

func (s *quicStream) Write(data []byte) (int, error) {
	return s.stream.Write(data)
}

func (s *quicStream) Close() error {
	return s.stream.Close()
}
