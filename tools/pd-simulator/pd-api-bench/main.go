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

package apibench

import (
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	flag "github.com/spf13/pflag"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/tools/pd-simulator/pd-api-bench/cases"
	"github.com/tikv/pd/tools/pd-simulator/pd-api-bench/config"
	"github.com/tikv/pd/tools/pd-simulator/pd-api-bench/metrics"
	"go.uber.org/zap"
	"os"
)

var coordinator *cases.Coordinator

func Run(ctx context.Context, cfg *config.Config) {
	defer logutil.LogPanic()

	if cfg == nil {
		cfg = config.NewConfig()
		err := cfg.Parse(os.Args[1:])
		switch errors.Cause(err) {
		case nil:
		case flag.ErrHelp:
			os.Exit(0)
		default:
			log.Fatal("[api] parse cmd flags error", zap.Error(err))
		}
	}

	metrics.RegisterMetrics()
	coordinator = cases.NewCoordinator(ctx, cfg)
}

func GetCoordinator() *cases.Coordinator {
	return coordinator
}
