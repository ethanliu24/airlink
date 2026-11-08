
package transfer

import (
	"airlink/internal/file"
	"io"
	"net"
	"os"
	"testing/fstest"
)

func testAddr() net.Addr {
	return &net.UDPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: 54321,
	}
}

type mockStream struct {
	writeFunc func([]byte) (int, error)
	readFunc  func([]byte) (int, error)
	closeFunc func() error
	abortFunc func()
}

func (s *mockStream) Read(data []byte) (int, error) {
	if s.readFunc == nil {
		return 0, io.EOF
	}

	return s.readFunc(data)
}

func (s *mockStream) Write(data []byte) (int, error) {
	if s.writeFunc == nil {
		return len(data), nil
	}

	return s.writeFunc(data)
}

func (s *mockStream) Close() error {
	if s.closeFunc == nil {
		return nil
	}

	return s.closeFunc()
}

func (s *mockStream) Abort() {
	if s.abortFunc != nil {
		s.abortFunc()
	}
}

type mockReader struct {
	data      string
	pointer   int
	closeFunc func() error
}

func (r *mockReader) Read(p []byte) (int, error) {
	if r.pointer >= len(r.data) {
		return 0, io.EOF
	}

	remaining := len(r.data) - r.pointer
	bytesToRead := min(len(p), remaining)

	n := copy(p, r.data[r.pointer:r.pointer+bytesToRead])
	r.pointer += n

	return n, nil
}

func (r *mockReader) Close() error {
	if r.closeFunc != nil {
		return r.closeFunc()
	}

	return nil
}

func (r *mockReader) Stat() (os.FileInfo, error) {
	mockFS := fstest.MapFS{
		"report.txt": &fstest.MapFile{
			Data: []byte("hello world"),
			Mode: 0644,
		},
	}

	return mockFS.Stat("report.txt")
}

func (r *mockReader) Seek(offset int64, whence int) (int64, error) {
	var target int64

	switch whence {
	case io.SeekStart:
		target = offset
	case io.SeekCurrent:
		target = int64(r.pointer) + offset
	case io.SeekEnd:
		target = int64(len(r.data)) + offset
	default:
		return 0, io.ErrUnexpectedEOF
	}

	if target < 0 {
		return 0, io.ErrUnexpectedEOF
	}

	r.pointer = int(target)

	return target, nil
}

func newMockReader(data string) (file.Reader, error) {
	return &mockReader{
		data:    data,
		pointer: 0,
	}, nil
}
