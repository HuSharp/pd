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
	"github.com/pingcap/log"
	flag "github.com/spf13/pflag"
	analysis "github.com/tikv/pd/tools/pd-dev/pd-analysis"
	backup "github.com/tikv/pd/tools/pd-dev/pd-backup"
	ut "github.com/tikv/pd/tools/pd-dev/pd-ut"
	region "github.com/tikv/pd/tools/pd-dev/regions-dump"
	store "github.com/tikv/pd/tools/pd-dev/stores-dump"
	"go.uber.org/zap"
	"os"
)

func switchDevelopmentMode(mode string) {
	log.Info("pd-dev start", zap.String("mode", mode))
	switch mode {
	case "ut":
		ut.Run()
	case "backup":
		backup.Run()
	case "regions-dump":
		region.Run()
	case "stores-dump":
		store.Run()
	case "analysis":
		analysis.Run()
	default:
		log.Fatal("please input a mode to run, or provide a config file to run all modes")
	}
}

func main() {
	// parse first argument
	if len(os.Args) < 2 {
		log.Fatal("please input a mode to run, or provide a config file to run all modes")
	}

	var mode string
	fs := flag.NewFlagSet("pd-dev", flag.ContinueOnError)
	fs.StringVar(&mode, "mode", "", "mode to run")
	fs.Parse(os.Args[1:])
	switchDevelopmentMode(mode)

	log.Info("pd-dev Exit")
}

func exit(code int) {
	os.Exit(code)
}
