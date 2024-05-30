// Copyright 2017 TiKV Project Authors.
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

package simulator

import (
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	flag "github.com/spf13/pflag"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/tools/pd-simulator/kv-simulator/simulator/config"
	"go.uber.org/zap"
	"os"
	"time"
)

var coordinator *Coordinator

func Run(ctx context.Context, cfg *config.SimConfig) {
	log.Info("[kv-simulator] simulator start", zap.Any("cfg", cfg))
	defer logutil.LogPanic()
	var err error

	if cfg == nil {
		cfg = config.NewSimConfig()
		err = cfg.Parse(os.Args[1:])
		cfg.PDAddrs = ""
		switch errors.Cause(err) {
		case nil:
		case flag.ErrHelp:
			os.Exit(0)
		default:
			log.Fatal("[kv-simulator] parse cmd flags error", zap.Error(err))
		}
	}

	// wait PD start. Otherwise, it will happen error when getting cluster ID.
	time.Sleep(3 * time.Second)
	coordinator = newCoordinator(ctx, cfg)
}

func GetCoordinator() *Coordinator {
	return coordinator
}
