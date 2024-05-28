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

package middlewares

import (
	simulator "github.com/tikv/pd/tools/pd-simulator/kv-simulator"
	"github.com/tikv/pd/tools/pd-simulator/pd-api-bench/cases"
	heartbeatbench "github.com/tikv/pd/tools/pd-simulator/pd-heartbeat-bench"
	tsobench "github.com/tikv/pd/tools/pd-simulator/pd-tso-bench"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
)

const (
	APICoordinatorContextKey       = "api_coordinator"
	HeartbeatCoordinatorContextKey = "heartbeat_coordinator"
	TSOCoordinatorContextKey       = "tso_coordinator"
	SimulatorCoordinatorContextKey = "simulator_coordinator"
)

// use sync.Once to ensure the coordinator is initialized only once
var apiCoordinatorOnce sync.Once

// BootstrapAPIChecker
func BootstrapAPIChecker() gin.HandlerFunc {
	return func(c *gin.Context) {
		co := c.MustGet(APICoordinatorContextKey).(*cases.Coordinator)
		if co == nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, "api coordinator is not bootstrapped")
			return
		}
		apiCoordinatorOnce.Do(func() {
			co.Run()
		})
		c.Next()
	}
}

// BootstrapHeartbeatChecker
func BootstrapHeartbeatChecker() gin.HandlerFunc {
	return func(c *gin.Context) {
		co := c.MustGet(HeartbeatCoordinatorContextKey).(*heartbeatbench.Coordinator)
		if co == nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, "heartbeat coordinator is not bootstrapped")
			return
		}
		c.Next()
	}
}

// BootstrapTSOChecker
func BootstrapTSOChecker() gin.HandlerFunc {
	return func(c *gin.Context) {
		co := c.MustGet(TSOCoordinatorContextKey).(*tsobench.Coordinator)
		if co == nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, "tso coordinator is not bootstrapped")
			return
		}
		c.Next()
	}
}

// BootstrapSimulatorChecker
func BootstrapSimulatorChecker() gin.HandlerFunc {
	return func(c *gin.Context) {
		co := c.MustGet(SimulatorCoordinatorContextKey).(*simulator.Coordinator)
		if co == nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, "simulator coordinator is not bootstrapped")
			return
		}
		c.Next()
	}
}
