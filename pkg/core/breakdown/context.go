// Copyright 2024 TiKV Project Authors.
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

package breakdown

import (
	"context"

	"github.com/tikv/pd/pkg/ratelimit"
)

// MetaProcessContext is a context for meta process.
type MetaProcessContext struct {
	context.Context
	Tracer     ProcessTracer
	TaskRunner ratelimit.Runner
	Limiter    *ratelimit.ConcurrencyLimiter
}

// ContextTODO creates a new MetaProcessContext.
// used in tests, can be changed if no need to test concurrency.
func ContextTODO() *MetaProcessContext {
	return &MetaProcessContext{
		Context:    context.TODO(),
		Tracer:     NewNoopHeartbeatProcessTracer(),
		TaskRunner: ratelimit.NewSyncRunner(),
		// Limit default is nil
	}
}

// CheckerContextTODO creates a new MetaProcessContext.
// used in tests, can be changed if no need to test concurrency.
func CheckerContextTODO() *MetaProcessContext {
	return &MetaProcessContext{
		Context:    context.TODO(),
		Tracer:     NewNoopCheckerProcessTracer(),
		TaskRunner: ratelimit.NewSyncRunner(),
		// Limit default is nil
	}
}
