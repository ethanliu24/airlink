package wire

import "io"

type errorWriter struct {
	err error
}

func (w *errorWriter) Write([]byte) (int, error) {
	return 0, w.err
}

type failAfterNWriter struct {
	n   int
	err error
}

func (w *failAfterNWriter) Write(p []byte) (int, error) {
	if w.n == 0 {
		return 0, w.err
	}

	n := min(len(p), w.n)
	w.n -= n

	if n < len(p) {
		return n, w.err
	}

	return n, nil
}

type errorReader struct {
	err error
}

func (r *errorReader) Read([]byte) (int, error) {
	return 0, r.err
}

type chunkedReader struct {
	reader    io.Reader
	chunkSize int
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	if len(p) > r.chunkSize {
		p = p[:r.chunkSize]
	}

	return r.reader.Read(p)
}
