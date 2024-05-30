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
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	simulator "github.com/tikv/pd/tools/pd-simulator/kv-simulator"
	simulator2 "github.com/tikv/pd/tools/pd-simulator/kv-simulator/simulator"
	"go.uber.org/zap"
	"io"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/tikv/pd/tools/pd-simulator/api/middlewares"
)

// RegisterSimulator register the API bench router.
func RegisterSimulator(r *gin.RouterGroup) {
	router := r.Group("kvsimulator")
	router.Use(middlewares.BootstrapSimulatorChecker())
	router.GET("/case", getSimulatorCase)
	router.POST("/case/:name", updateSimulatorCase)
	router.POST("/config", updateSimulatorConfig)
	router.GET("/config", GetSimulatorConfig)
	router.POST("/event/:name/", createSimulatorEvent)

}

func createSimulatorEvent(c *gin.Context) {
	co := c.MustGet(middlewares.SimulatorCoordinatorContextKey).(*simulator.Coordinator)
	er := co.GetEventRunner()
	event := c.Param("name")
	if len(event) < 1 {
		c.AbortWithStatusJSON(http.StatusBadRequest, errors.New("event is required"))
		return
	}
	switch event {
	case "add-node":
		er.AddEvent(&simulator2.AddNode{})
	case "down-node":
		id := c.Query("node-id")
		var ID int
		if len(id) != 0 {
			ID, _ = strconv.Atoi(id)
		}
		er.AddEvent(&simulator2.DownNode{ID: ID})
	default:
		c.AbortWithStatusJSON(http.StatusBadRequest, fmt.Sprintf("event %s is not supported", event))
	}
	c.IndentedJSON(http.StatusOK, "Successfully create event")
}

func getSimulatorCase(c *gin.Context) {
	log.Info("[pd-simulator] get simulator case")
	co := c.MustGet(middlewares.SimulatorCoordinatorContextKey)
	log.Info("[pd-simulator] get simulator case", zap.Any("case", co.(*simulator.Coordinator).GetCase()))
	c.IndentedJSON(http.StatusOK, co.(*simulator.Coordinator).GetCase())
}

func updateSimulatorCase(c *gin.Context) {
	co := c.MustGet(middlewares.SimulatorCoordinatorContextKey).(*simulator.Coordinator)
	if co == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, "Simulator coordinator not found")
		return
	}
	name := c.Param("name")
	if err := co.UpdateDriver(name); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errors.ErrorStack(err))
		return
	}

	c.IndentedJSON(http.StatusOK, "Successfully updated the case")
}

func GetSimulatorConfig(c *gin.Context) {
	co := c.MustGet(middlewares.SimulatorCoordinatorContextKey)
	if co == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, "Simulator coordinator not found")
		return
	}
	c.IndentedJSON(http.StatusOK, co.(*simulator.Coordinator).GetConfig())
}

func updateSimulatorConfig(c *gin.Context) {
	co := c.MustGet(middlewares.SimulatorCoordinatorContextKey).(*simulator.Coordinator)
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
	oldCfg := co.GetConfig().Clone()
	for k, v := range config {
		if err := co.UpdateConfig(oldCfg, k, v); err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, errors.ErrorStack(err))
			return
		}
	}

	c.IndentedJSON(http.StatusOK, "Successfully updated the configuration")
}
