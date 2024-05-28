// Copyright 2022 TiKV Project Authors.
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

package api

import (
	"context"
	"errors"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"go.uber.org/zap"
	"net/http"
	"os"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/utils/tempurl"
	svrcfg "github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tools/pd-simulator/api/handlers"
	"github.com/tikv/pd/tools/pd-simulator/api/middlewares"
	simulator "github.com/tikv/pd/tools/pd-simulator/kv-simulator"
	"github.com/tikv/pd/tools/pd-simulator/kv-simulator/simulator/config"
	api "github.com/tikv/pd/tools/pd-simulator/pd-api-bench"
	"github.com/tikv/pd/tools/pd-simulator/pd-api-bench/cases"
	apicfg "github.com/tikv/pd/tools/pd-simulator/pd-api-bench/config"
	heartbeat "github.com/tikv/pd/tools/pd-simulator/pd-heartbeat-bench"
	tso "github.com/tikv/pd/tools/pd-simulator/pd-tso-bench"
	"github.com/tikv/pd/tools/pd-simulator/util"
)

const prefix = "/pd/simulator"

// RunHTTPServer runs an HTTP server to provide the PD simulator API.
func RunHTTPServer(addr string) *http.Server {
	log.Info("[pd-simulator] http server is running", zap.String("address", addr))
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(middlewares.APICoordinatorContextKey, api.GetCoordinator())
		c.Set(middlewares.HeartbeatCoordinatorContextKey, heartbeat.GetCoordinator())
		c.Set(middlewares.TSOCoordinatorContextKey, tso.GetCoordinator())
		c.Set(middlewares.SimulatorCoordinatorContextKey, simulator.GetCoordinator())
		c.Next()
	})
	root := router.Group(prefix)
	root.Use(gin.Recovery())

	// profile API
	root.GET("/metrics", utils.PromHandler())
	pprof.Register(router)

	handlers.RegisterAPIBench(root)
	handlers.RegisterTSOBench(root)
	handlers.RegisterHeartbeatBench(root)
	handlers.RegisterSimulator(root)

	// TODO: remove debug things
	root.GET("/config", showConfig)
	root.StaticFile("/test", "/Users/pingcap/CS/PingCAP/pd/tools/pd-simulator/api/test.html")

	srv := &http.Server{Addr: addr, Handler: router.Handler(), ReadHeaderTimeout: 3 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal("[pd-simulator] server listen error", zap.Error(err))
		}
	}()

	return srv
}

func StopServer(srv *http.Server) {
	if co := api.GetCoordinator(); co != nil {
		co.Stop()
		ReadConfig.API = co.GetConfig()
	}
	if co := heartbeat.GetCoordinator(); co != nil {
		co.Stop()
		ReadConfig.Heartbeat = co.GetConfig()
	}
	if co := tso.GetCoordinator(); co != nil {
		co.Stop()
		ReadConfig.TSO = co.GetConfig()
	}
	if co := simulator.GetCoordinator(); co != nil {
		co.Stop()
		ReadConfig.Simulator = co.GetConfig()
	}

	if err := srv.Shutdown(context.Background()); err != nil {
		log.Fatal("[pd-simulator] server shutdown error", zap.Error(err))
	}
}

var (
	ReadConfig = newConfig()
	InitConfig = newConfig()
	testPath   = "/Users/pingcap/CS/PingCAP/pd/tools/pd-simulator/util/example2.toml"
)

type Config struct {
	util.GeneralConfig
	API       *apicfg.Config
	TSO       *tso.Config
	Heartbeat *heartbeat.Config
	Simulator *config.SimConfig
}

func newConfig() Config {
	return Config{
		GeneralConfig: util.GeneralConfig{},
		API:           &apicfg.Config{},
		TSO:           &tso.Config{},
		Heartbeat:     &heartbeat.Config{},
		Simulator:     &config.SimConfig{},
	}
}

func DecodeConfig(file string) Config {
	cfg := newConfig()
	if _, err := toml.DecodeFile(file, &cfg); err != nil {
		log.Fatal("[pd-simulator] decode config file failed", zap.Error(err))

	}
	cfg.API.GeneralConfig = &cfg.GeneralConfig
	cfg.API.HTTP = make(map[string]apicfg.CaseConfig)
	cfg.API.GRPC = make(map[string]apicfg.CaseConfig)
	cfg.API.ETCD = make(map[string]apicfg.CaseConfig)
	// add default config
	cfg.TSO.GeneralConfig = &cfg.GeneralConfig
	cfg.Heartbeat.GeneralConfig = &cfg.GeneralConfig
	cfg.Simulator.GeneralConfig = &cfg.GeneralConfig
	cfg.Simulator.ServerConfig = &svrcfg.Config{
		Name:       "pd",
		ClientUrls: tempurl.Alloc(),
		PeerUrls:   tempurl.Alloc(),
		Replication: sc.ReplicationConfig{
			MaxReplicas: 3,
		},
	}

	return cfg
}

func WriteConfigFile() {
	file, err := os.Create(testPath)
	if err != nil {
		log.Fatal("[pd-simulator] create config file failed", zap.Error(err))
	}
	defer file.Close()

	encoder := toml.NewEncoder(file)
	// remove redundant fields
	ReadConfig.API.GeneralConfig = nil
	ReadConfig.TSO.GeneralConfig = nil
	ReadConfig.Heartbeat.GeneralConfig = nil
	ReadConfig.Simulator.GeneralConfig = nil
	ReadConfig.Simulator.ServerConfig = nil
	if err := encoder.Encode(ReadConfig); err != nil {
		log.Fatal("[pd-simulator] encode config file failed", zap.Error(err))
	}
}

// TODO: remove it
func showConfig(c *gin.Context) {
	var cfg []any
	co := c.MustGet(middlewares.APICoordinatorContextKey)
	if co == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, "APICoordinatorContextKey is nil")
		return
	}
	cfg = append(cfg, co.(*cases.Coordinator).GetConfig())
	co = c.MustGet(middlewares.TSOCoordinatorContextKey)
	if co == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, "TSO coordinator not found")
		return
	}
	cfg = append(cfg, co.(*tso.Coordinator).GetConfig())

	co = c.MustGet(middlewares.HeartbeatCoordinatorContextKey)
	if co == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, "Heartbeat coordinator not found")
		return
	}
	cfg = append(cfg, co.(*heartbeat.Coordinator).GetConfig())

	co = c.MustGet(middlewares.SimulatorCoordinatorContextKey)
	if co == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, "Simulator coordinator not found")
		return
	}
	cfg = append(cfg, co.(*simulator.Coordinator).GetConfig())
	c.IndentedJSON(http.StatusOK, cfg)
}
