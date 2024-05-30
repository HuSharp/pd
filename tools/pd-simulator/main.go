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

package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	flag "github.com/spf13/pflag"
	router "github.com/tikv/pd/tools/pd-simulator/api"
	simulator "github.com/tikv/pd/tools/pd-simulator/kv-simulator"
	api "github.com/tikv/pd/tools/pd-simulator/pd-api-bench"
	heartbeat "github.com/tikv/pd/tools/pd-simulator/pd-heartbeat-bench"
	tso "github.com/tikv/pd/tools/pd-simulator/pd-tso-bench"
	"github.com/tikv/pd/tools/pd-simulator/util"
	"go.uber.org/zap"
)

var allComponents = []string{"api", "tso", "heartbeat"}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	var sig os.Signal
	go func() {
		sig = <-sc
		cancel()
	}()

	fs := flag.NewFlagSet("pd-simulator", flag.ExitOnError)
	// ignore some undefined flag
	fs.ParseErrorsWhitelist.UnknownFlags = true
	// read config file
	cfg := util.NewGeneralConfig(fs)
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Fatal("[pd-simulator] parse cmd flags error", zap.Error(err))
	}
	var wg sync.WaitGroup
	// get config firstly
	if cfg.ConfigFile == "" {
		log.Info("[pd-simulator] using default config")
		// Run KV Simulator firstly with `default` case, which just build
		simulator.Run(ctx, nil)
		co := simulator.GetCoordinator()
		// update PDAddrs
		go co.Run("")
		// using InitConfig
		router.InitConfig.PDAddrs = co.GetConfig().PDAddrs

		// check mode
		if cfg.Mode != "" {
			// run single mode
			wg.Add(1)
			go switchDevelopmentMode(ctx, cfg.Mode, router.InitConfig, &wg)
		} else {
			// run all components
			for _, component := range allComponents {
				wg.Add(1)
				go switchDevelopmentMode(ctx, component, router.InitConfig, &wg)
			}
		}
	} else {
		log.Info("[pd-simulator] using config file", zap.String("file", cfg.ConfigFile))
		// using ReadConfig
		router.ReadConfig = router.DecodeConfig(cfg, cfg.ConfigFile)
		// Run KV Simulator
		simulator.Run(ctx, router.ReadConfig.Simulator)
		co := simulator.GetCoordinator()
		go co.Run("")
		if router.ReadConfig.PDAddrs == "" {
			router.ReadConfig.PDAddrs = co.GetConfig().PDAddrs
		}

		// check mode
		if cfg.Mode != "" {
			// run single mode
			wg.Add(1)
			go switchDevelopmentMode(ctx, cfg.Mode, router.ReadConfig, &wg)
		} else {
			// run all components
			for _, component := range allComponents {
				wg.Add(1)
				go switchDevelopmentMode(ctx, component, router.ReadConfig, &wg)
			}
		}
	}

	wg.Wait()
	// wait for all components start
	srv := router.RunHTTPServer(cfg.Status)

	<-ctx.Done()
	// retain the config file
	router.StopServer(srv)
	router.WriteConfigFile()
	log.Info("pd-simulator Exit")

	switch sig {
	case syscall.SIGTERM:
		exit(0)
	default:
		exit(1)
	}
}

func switchDevelopmentMode(ctx context.Context, mode string, cfg router.Config, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Info("[pd-simulator] start", zap.String("mode", mode))
	switch mode {
	case "api":
		api.Run(ctx, cfg.API)
	case "tso":
		tso.Run(ctx, cfg.TSO)
	case "heartbeat":
		heartbeat.Run(ctx, cfg.Heartbeat)
	default:
		log.Fatal("[pd-simulator] please input a mode to run, or provide a config file to run all modes")
	}
}

func exit(code int) {
	os.Exit(code)
}
