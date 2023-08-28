package retry

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"go.uber.org/multierr"
)

// BackOffer is a backoff policy for retrying operations.
type BackOffer struct {
	maxBackoff  time.Duration
	nextBackoff time.Duration
}

// WithBackoff is a helper function to retry a function with backoff.
func WithBackoff(
	ctx context.Context,
	fn func() error,
	bo *BackOffer,
) error {
	var allErrors error
	err := fn()
	if err != nil {
		log.Info("back offer retry", zap.Error(err))
		allErrors = multierr.Append(allErrors, err)
		select {
		case <-ctx.Done():
			log.Info("back offer context done", zap.Error(ctx.Err()))
			return allErrors
		case <-time.After(bo.NextBackoff()):
			log.Info("back offer next backoff", zap.Error(ctx.Err()))
		}
	} else {
		bo.ResetBackoff()
		return nil
	}
	return allErrors
}

// InitialBackOffer make the initial state for retrying.
func InitialBackOffer(initialBackoff, maxBackoff time.Duration) BackOffer {
	return BackOffer{
		nextBackoff: initialBackoff,
		maxBackoff:  maxBackoff,
	}
}

// NextBackoff implements the `Backoffer`, for now use the `ExponentialBackoff`.
func (rs *BackOffer) NextBackoff() time.Duration {
	return rs.ExponentialBackoff()
}

// ExponentialBackoff Get the exponential backoff duration.
func (rs *BackOffer) ExponentialBackoff() time.Duration {
	backoff := rs.nextBackoff
	rs.nextBackoff *= 2
	if rs.nextBackoff > rs.maxBackoff {
		rs.nextBackoff = rs.maxBackoff
	}
	return backoff
}

// ResetBackoff reset the backoff to initial state.
func (rs *BackOffer) ResetBackoff() {
	rs.nextBackoff = 0
}
