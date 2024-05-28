// Copyright 2019 TiKV Project Authors.
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

package heartbeatbench

import (
	"context"
	"math/rand"
	"os"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/pflag"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/utils/logutil"
	"go.uber.org/zap"
)

var coordinator *Coordinator

func Run(ctx context.Context, cfg *Config) {
	defer logutil.LogPanic()

	rand.New(rand.NewSource(0)) // Ensure consistent behavior multiple times
	statistics.Denoising = false

	if cfg == nil {
		cfg = newConfig()
		err := cfg.Parse(os.Args[1:])
		switch errors.Cause(err) {
		case nil:
		case pflag.ErrHelp:
			os.Exit(0)
		default:
			log.Fatal("[heartbeat] parse cmd flags error", zap.Error(err))
		}
	}

	maxVersion = cfg.InitEpochVer
	// let PD have enough time to start
	time.Sleep(5 * time.Second)
	coordinator = newCoordinator(ctx, cfg)
}

func GetCoordinator() *Coordinator {
	return coordinator
}
