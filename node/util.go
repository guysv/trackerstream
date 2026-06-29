package tsnode

import (
	"crypto/rand"
	"encoding/binary"
	"time"
)

// jitter returns base ± up to spread, using crypto/rand (Math.random is unavailable to us
// here and we want no shared PRNG state across goroutines). Used to de-synchronize the
// keepalive loop so warm peers don't reconnect in a thundering herd.
func jitter(base, spread time.Duration) time.Duration {
	if spread <= 0 {
		return base
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	n := int64(binary.LittleEndian.Uint64(b[:]) % uint64(2*spread))
	return base - spread + time.Duration(n)
}
