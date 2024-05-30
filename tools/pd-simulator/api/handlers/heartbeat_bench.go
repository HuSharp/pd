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

package handlers

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/errors"
	"github.com/tikv/pd/tools/pd-simulator/api/middlewares"
	heartbeat "github.com/tikv/pd/tools/pd-simulator/pd-heartbeat-bench"
	"io"
	"net/http"
)

// RegisterHeartbeatBench register the API bench router.
func RegisterHeartbeatBench(r *gin.RouterGroup) {
	router := r.Group("heartbeat")
	router.Use(middlewares.BootstrapHeartbeatChecker())
	router.POST("/start", startHeartbeatServer)
	router.POST("/stop", stopHeartbeatServer)
	router.POST("/config", updateHeartbeatConfig)
	router.GET("/config", GetHeartbeatConfig)
}

// CreateHeartbeatServer creates all HTTP cases.
//
// @Tags
// @Summary
// @Param    body  body  map[string]cases.Config  true  "HTTP cases"
// @Produce  json
// @Success  200  {object}  string  "Success"
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/http/all [post]
func startHeartbeatServer(c *gin.Context) {
	co := c.MustGet(middlewares.HeartbeatCoordinatorContextKey).(*heartbeat.Coordinator)
	go co.Run()
	c.IndentedJSON(http.StatusOK, "Success")
}

func stopHeartbeatServer(c *gin.Context) {
	co := c.MustGet(middlewares.HeartbeatCoordinatorContextKey).(*heartbeat.Coordinator)
	co.Stop()
	c.IndentedJSON(http.StatusOK, "Success")
}

func updateHeartbeatConfig(c *gin.Context) {
	co := c.MustGet(middlewares.HeartbeatCoordinatorContextKey).(*heartbeat.Coordinator)
	if co == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, "Heartbeat coordinator not found")
		return
	}

	data, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errors.ErrorStack(err))
		return
	}

	var config map[string]any
	if err := json.Unmarshal(data, &config); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errors.ErrorStack(err))
		return
	}
	oldCfg := co.Config.Clone()
	for k, v := range config {
		if err := co.Config.UpdateConfig(oldCfg, k, v); err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, errors.ErrorStack(err))
			return
		}
	}

	c.IndentedJSON(http.StatusOK, "Successfully updated the configuration")
}

func GetHeartbeatConfig(c *gin.Context) {
	co := c.MustGet(middlewares.HeartbeatCoordinatorContextKey)
	if co == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, "Heartbeat coordinator not found")
		return
	}

	c.IndentedJSON(http.StatusOK, co.(*heartbeat.Coordinator).GetConfig())
}
