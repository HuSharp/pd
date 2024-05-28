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

package config

import (
	flag "github.com/spf13/pflag"
	"github.com/tikv/pd/tools/pd-simulator/util"
)

// Config is the heartbeat-bench configuration.
type Config struct {
	*util.GeneralConfig
	Client int64 `toml:"client" json:"client"`

	QPS       int64
	Burst     int64
	HTTPCases string
	GRPCCases string

	HTTP map[string]CaseConfig `toml:"http" json:"http"`
	GRPC map[string]CaseConfig `toml:"grpc" json:"grpc"`
	ETCD map[string]CaseConfig `toml:"etcd" json:"etcd"`
}

// NewConfig return a set of settings.
func NewConfig() *Config {
	cfg := &Config{
		GeneralConfig: &util.GeneralConfig{},
	}
	cfg.FlagSet = flag.NewFlagSet("api-bench", flag.ContinueOnError)
	fs := cfg.FlagSet
	cfg.GeneralConfig = util.NewGeneralConfig(fs)
	cfg.Mode = "api"
	fs.ParseErrorsWhitelist.UnknownFlags = true
	fs.Int64Var(&cfg.Client, "client", 1, "client number")
	fs.Int64Var(&cfg.QPS, "qps", 1, "qps")
	fs.Int64Var(&cfg.Burst, "burst", 1, "burst")
	fs.StringVar(&cfg.HTTPCases, "http-cases", "", "http api cases")
	fs.StringVar(&cfg.GRPCCases, "grpc-cases", "", "grpc cases")

	cfg.HTTP = make(map[string]CaseConfig)
	cfg.GRPC = make(map[string]CaseConfig)
	cfg.ETCD = make(map[string]CaseConfig)
	return cfg
}

// CaseConfig is the configuration for the case.
type CaseConfig struct {
	QPS   int64 `toml:"qps" json:"qps"`
	Burst int64 `toml:"burst" json:"burst"`
}

func NewCaseConfig() *CaseConfig {
	return &CaseConfig{
		Burst: 1,
	}
}

// Clone returns a cloned configuration.
func (c *CaseConfig) Clone() *CaseConfig {
	cfg := *c
	return &cfg
}
