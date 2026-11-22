package node

import (
	"time"

	"github.com/quic-go/quic-go"
)

// TODO properly config this
func getQuicConfig() *quic.Config {
	return &quic.Config{
		// InitialStreamReceiveWindow: 1<<20, // 1 MB
		// MaxStreamReceiveWindow: 6<<20, // 6 MB
		// InitialConnectionReceiveWindow: 2<<20, // 2 MB
		// MaxConnectionReceiveWindow: 12<<20, // 12 MB
		// MaxIncomingStreams: 100, // bidirectional streams
		// MaxIncomingUniStreams: 100, // unidirectional streams
		MaxIdleTimeout:  30 * time.Second,
		KeepAlivePeriod: 15 * time.Second,
	}
}
