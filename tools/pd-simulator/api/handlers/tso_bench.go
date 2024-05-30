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
	"github.com/pingcap/errors"
	tsobench "github.com/tikv/pd/tools/pd-simulator/pd-tso-bench"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/tikv/pd/tools/pd-simulator/api/middlewares"
)

// RegisterTSOBench register the API bench router.
func RegisterTSOBench(r *gin.RouterGroup) {
	router := r.Group("tso")
	router.Use(middlewares.BootstrapTSOChecker())
	router.POST("/start", startTSOServer)
	router.POST("/stop", stopTSOServer)
	router.POST("/config", updateTSOConfig)
	router.GET("/config", GetTSOConfig)
}

func startTSOServer(c *gin.Context) {
	co := c.MustGet(middlewares.TSOCoordinatorContextKey).(*tsobench.Coordinator)
	go co.Run()
	c.IndentedJSON(http.StatusOK, "Success")
}

func stopTSOServer(c *gin.Context) {
	co := c.MustGet(middlewares.TSOCoordinatorContextKey).(*tsobench.Coordinator)
	co.Stop()
	c.IndentedJSON(http.StatusOK, "Success")
}

func GetTSOConfig(c *gin.Context) {
	co := c.MustGet(middlewares.TSOCoordinatorContextKey)
	if co == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, "TSO coordinator not found")
		return
	}
	c.IndentedJSON(http.StatusOK, co.(*tsobench.Coordinator).GetConfig())
}

func updateTSOConfig(c *gin.Context) {
	co := c.MustGet(middlewares.TSOCoordinatorContextKey).(*tsobench.Coordinator)
	data, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errors.ErrorStack(err))
		return
	}

	var config map[string]any
	if err = json.Unmarshal(data, &config); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errors.ErrorStack(err))
		return
	}
	oldCfg := co.GetConfig().Clone()
	for k, v := range config {
		if err = co.UpdateConfig(oldCfg, k, v); err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, errors.ErrorStack(err))
			return
		}
	}

	c.IndentedJSON(http.StatusOK, "Successfully updated the configuration")
}
