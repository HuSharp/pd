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

package handlers

import (
	"github.com/tikv/pd/tools/pd-simulator/pd-api-bench/config"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/tikv/pd/tools/pd-simulator/api/middlewares"
	"github.com/tikv/pd/tools/pd-simulator/pd-api-bench/cases"
)

// RegisterAPIBench register the API bench router.
func RegisterAPIBench(r *gin.RouterGroup) {
	router := r.Group("api")
	router.Use(middlewares.BootstrapAPIChecker())
	router.GET("/config", GetAPIConfig)
	router.POST("/http/all", CreateAllHTTPCases)
	router.POST("/http/:name", CreateHTTPCase)
	router.POST("/grpc/all", CreateAllGRPCCases)
	router.POST("/grpc/:name", CreateGRPCCase)
	router.POST("/etcd/all", CreateAllEtcdCases)
	router.POST("/etcd/:name", CreateEtcdCase)
	router.GET("/http/all", GetALLHTTPCases)
	router.GET("/http/:name", GetHTTPCase)
	router.GET("/grpc/all", GetALLGRPCCases)
	router.GET("/grpc/:name", GetGRPCCase)
	router.GET("/etcd/all", GetALLEtcdCases)
	router.GET("/etcd/:name", GetEtcdCase)
	router.DELETE("/http/:name", DeleteHTTPCase)
	router.DELETE("/grpc/:name", DeleteGRPCCase)
	router.DELETE("/etcd/:name", DeleteEtcdCase)
}

func GetAPIConfig(c *gin.Context) {
	co := c.MustGet(middlewares.APICoordinatorContextKey)
	if co == nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, "APICoordinatorContextKey is nil")
		return
	}
	c.IndentedJSON(http.StatusOK, co.(*cases.Coordinator).GetConfig())
}

// CreateAllHTTPCases creates all HTTP cases.
//
// @Tags
// @Summary
// @Param    body  body  map[string]cases.Config  true  "HTTP cases"
// @Produce  json
// @Success  200  {object}  string  "Success"
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/http/all [post]
func CreateAllHTTPCases(c *gin.Context) {
	co := c.MustGet(middlewares.APICoordinatorContextKey).(*cases.Coordinator)
	var input map[string]config.CaseConfig
	if err := c.ShouldBindJSON(&input); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, err.Error())
		return
	}
	for name, cfg := range input {
		if err := co.SetHTTPCase(name, &cfg); err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
			return
		}
	}
	c.IndentedJSON(http.StatusOK, "Success")
}

// CreateHTTPCase gets the HTTP case.
//
// @Tags
// @Summary
// @Param    name  path  string  true  "HTTP case name"
// @Param    qps  query  int  false  "QPS"
// @Param    burst  query  int  false  "Burst"
// @Produce  json
// @Success  200  {object}  string  "Success"
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/http/{name} [post]
func CreateHTTPCase(c *gin.Context) {
	co := c.MustGet(middlewares.APICoordinatorContextKey).(*cases.Coordinator)
	name := c.Param("name")
	cfg := getCfg(c)
	if err := co.SetHTTPCase(name, cfg); err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, "Success")

}

// CreateAllGRPCCases creates all gRPC cases.
//
// @Tags
// @Summary
// @Param    body  body  map[string]cases.Config  true  "gRPC cases"
// @Produce  json
// @Success  200  {object}  string  "Success"
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/grpc/all [post]
func CreateAllGRPCCases(c *gin.Context) {
	co := c.MustGet(middlewares.APICoordinatorContextKey).(*cases.Coordinator)
	var input map[string]config.CaseConfig
	if err := c.ShouldBindJSON(&input); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, err.Error())
		return
	}
	for name, cfg := range input {
		if err := co.SetGRPCCase(name, &cfg); err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
			return
		}
	}
	c.IndentedJSON(http.StatusOK, "Success")
}

// CreateGRPCCase gets the gRPC case.
//
// @Tags
// @Summary
// @Param    name  path  string  true  "gRPC case name"
// @Param    qps  query  int  false  "QPS"
// @Param    burst  query  int  false  "Burst"
// @Produce  json
// @Success  200  {object}  string  "Success"
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/grpc/{name} [post]
func CreateGRPCCase(c *gin.Context) {
	co := c.MustGet(middlewares.APICoordinatorContextKey).(*cases.Coordinator)
	name := c.Param("name")
	cfg := getCfg(c)
	if err := co.SetGRPCCase(name, cfg); err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, "Success")
}

// CreateAllEtcdCases creates all etcd cases.
//
// @Tags
// @Summary
// @Param    body  body  map[string]cases.Config  true  "etcd cases"
// @Produce  json
// @Success  200  {object}  string  "Success"
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/etcd/all [post]
func CreateAllEtcdCases(c *gin.Context) {
	co := c.MustGet(middlewares.APICoordinatorContextKey).(*cases.Coordinator)
	var input map[string]config.CaseConfig
	if err := c.ShouldBindJSON(&input); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, err.Error())
		return
	}
	for name, cfg := range input {
		if err := co.SetETCDCase(name, &cfg); err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
			return
		}
	}
	c.IndentedJSON(http.StatusOK, "Success")
}

// CreateEtcdCase gets the etcd case.
//
// @Tags
// @Summary
// @Param    name  path  string  true  "etcd case name"
// @Param    qps  query  int  false  "QPS"
// @Param    burst  query  int  false  "Burst"
// @Produce  json
// @Success  200  {object}  string  "Success"
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/etcd/{name} [post]
func CreateEtcdCase(c *gin.Context) {
	co := c.MustGet(middlewares.APICoordinatorContextKey).(*cases.Coordinator)
	name := c.Param("name")
	cfg := getCfg(c)
	if err := co.SetETCDCase(name, cfg); err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, cfg)
}

// GetALLHTTPCases gets all HTTP cases.
//
// @Tags
// @Summary
// @Produce  json
// @Success  200  {object}  map[string]cases.Config  "Success"
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/http/all [get]
func GetALLHTTPCases(c *gin.Context) {
	co := c.MustGet(middlewares.APICoordinatorContextKey).(*cases.Coordinator)
	ret := co.GetAllHTTPCases()
	c.IndentedJSON(http.StatusOK, ret)
}

// GetHTTPCase gets the HTTP case.
//
// @Tags
// @Summary
// @Param    name  path  string  true  "HTTP case name"
// @Produce  json
// @Success  200  {object}  cases.Config  "Success"
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/http/{name} [get]
func GetHTTPCase(c *gin.Context) {
	co := c.MustGet(middlewares.APICoordinatorContextKey).(*cases.Coordinator)
	name := c.Param("name")
	cfg, err := co.GetHTTPCase(name)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, cfg)
}

// GetALLGRPCCases gets all gRPC cases.
//
// @Tags
// @Summary
// @Produce  json
// @Success  200  {object}  map[string]cases.Config  "Success"
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/grpc/all [get]
func GetALLGRPCCases(c *gin.Context) {
	co := c.MustGet(middlewares.APICoordinatorContextKey).(*cases.Coordinator)
	ret := co.GetAllGRPCCases()
	c.IndentedJSON(http.StatusOK, ret)
}

// GetGRPCCase gets the gRPC case.
//
// @Tags
// @Summary
// @Param    name  path  string  true  "gRPC case name"
// @Produce  json
// @Success  200  {object}  cases.Config  "Success"
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/grpc/{name} [get]
func GetGRPCCase(c *gin.Context) {
	co := c.MustGet(middlewares.APICoordinatorContextKey).(*cases.Coordinator)
	name := c.Param("name")
	cfg, err := co.GetGRPCCase(name)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, cfg)
}

// GetALLEtcdCases gets all etcd cases.
//
// @Tags
// @Summary
// @Produce  json
// @Success  200  {object}  map[string]cases.Config  "Success"
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/etcd/all [get]
func GetALLEtcdCases(c *gin.Context) {
	co := c.MustGet(middlewares.APICoordinatorContextKey).(*cases.Coordinator)
	ret := co.GetAllETCDCases()
	c.IndentedJSON(http.StatusOK, ret)
}

// GetEtcdCase gets the etcd case.
//
// @Tags
// @Summary
// @Param    name  path  string  true  "etcd case name"
// @Produce  json
// @Success  200  {object}  cases.Config  "Success"
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/etcd/{name} [get]
func GetEtcdCase(c *gin.Context) {
	co := c.MustGet(middlewares.APICoordinatorContextKey).(*cases.Coordinator)
	name := c.Param("name")
	cfg, err := co.GetETCDCase(name)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, cfg)
}

// DeleteHTTPCase deletes the HTTP case.
//
// @Tags
// @Summary
// @Param    name  path  string  true  "HTTP case name"
// @Produce  json
// @Success  200  {object}  string  "Success"
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/http/{name} [delete]
func DeleteHTTPCase(c *gin.Context) {
	co := c.MustGet(middlewares.APICoordinatorContextKey).(*cases.Coordinator)
	name := c.Param("name")
	err := co.DeleteHTTPCase(name)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, "Success")
}

// DeleteGRPCCase deletes the gRPC case.
//
// @Tags
// @Summary
// @Param    name  path  string  true  "gRPC case name"
// @Produce  json
// @Success  200  {object}  string  "Success"
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/grpc/{name} [delete]
func DeleteGRPCCase(c *gin.Context) {
	co := c.MustGet(middlewares.APICoordinatorContextKey).(*cases.Coordinator)
	name := c.Param("name")
	err := co.DeleteGRPCCase(name)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, "Success")
}

// DeleteEtcdCase deletes the etcd case.
//
// @Tags
// @Summary
// @Param    name  path  string  true  "etcd case name"
// @Produce  json
// @Success  200  {object}  string  "Success"
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /config/etcd/{name} [delete]
func DeleteEtcdCase(c *gin.Context) {
	co := c.MustGet(middlewares.APICoordinatorContextKey).(*cases.Coordinator)
	name := c.Param("name")
	err := co.DeleteETCDCase(name)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, "Success")
}

func getCfg(c *gin.Context) *config.CaseConfig {
	var err error
	cfg := &config.CaseConfig{}
	qpsStr := c.Query("qps")
	if len(qpsStr) > 0 {
		cfg.QPS, err = strconv.ParseInt(qpsStr, 10, 64)
		if err != nil {
			c.String(http.StatusBadRequest, err.Error())
		}
	}
	burstStr := c.Query("burst")
	if len(burstStr) > 0 {
		cfg.Burst, err = strconv.ParseInt(burstStr, 10, 64)
		if err != nil {
			c.String(http.StatusBadRequest, err.Error())
		}
	}
	return cfg
}
