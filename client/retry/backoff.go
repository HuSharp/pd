// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package retry

import (
	"context"
	"time"

	"github.com/pingcap/failpoint"
	"go.uber.org/multierr"
)

// BackOffer is a backoff policy for retrying operations.
type BackOffer struct {
	maxBackoff  time.Duration
	nextBackoff time.Duration
}

// WithBackoff is a helper function to add backoff.
func WithBackoff(
	ctx context.Context,
	fn func() error,
	bo *BackOffer,
) error {
	var allErrors error
	err := fn()
	if err != nil {
		allErrors = multierr.Append(allErrors, err)
		select {
		case <-ctx.Done():
			return allErrors
		case <-time.After(bo.NextBackoff()):
			failpoint.Inject("backOffExecute", func() {
				testBackOffExecuteFlag = true
			})
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
		maxBackoff:  maxBackoff,
		nextBackoff: initialBackoff,
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

// Only used for test.
var testBackOffExecuteFlag = false

// TestBackOffExecute Only used for test.
func TestBackOffExecute() bool {
	return testBackOffExecuteFlag
}
