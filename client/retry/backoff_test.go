package retry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExponentialBackoff(t *testing.T) {
	re := require.New(t)

	baseBackoff := 100 * time.Millisecond
	maxBackoff := 1 * time.Second

	backoff := InitialBackOffer(baseBackoff, maxBackoff)
	re.Equal(backoff.NextBackoff(), baseBackoff)
	re.Equal(backoff.NextBackoff(), 2*baseBackoff)

	for i := 0; i < 10; i++ {
		re.LessOrEqual(backoff.NextBackoff(), maxBackoff)
	}
	re.Equal(backoff.NextBackoff(), maxBackoff)
}
