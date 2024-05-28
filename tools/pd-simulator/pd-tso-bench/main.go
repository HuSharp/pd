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

package tsobench

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/pflag"
	"github.com/tikv/pd/pkg/utils/logutil"
	"go.uber.org/zap"
)

const (
	keepaliveTime    = 10 * time.Second
	keepaliveTimeout = 3 * time.Second
)

var coordinator *Coordinator

func collectMetrics(server *httptest.Server) string {
	time.Sleep(1100 * time.Millisecond)
	res, _ := http.Get(server.URL)
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	return string(body)
}

func Run(ctx context.Context, cfg *Config) {
	defer logutil.LogPanic()

	if cfg == nil {
		cfg = newConfig()
		err := cfg.Parse(os.Args[1:])
		switch errors.Cause(err) {
		case nil:
		case pflag.ErrHelp:
			os.Exit(0)
		default:
			log.Fatal("[tso] parse cmd flags error", zap.Error(err))
		}
	}

	coordinator = newCoordinator(ctx, cfg)
}

func GetCoordinator() *Coordinator {
	return coordinator
}
