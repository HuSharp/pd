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

package cases

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	pdHttp "github.com/tikv/pd/client/http"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

var base = int64(time.Second) / int64(time.Microsecond)

type httpController struct {
	HTTPCase
	clients []pdHttp.Client
	pctx    context.Context

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newHTTPController(ctx context.Context, clis []pdHttp.Client, fn HTTPCreateFn) *httpController {
	c := &httpController{
		pctx:     ctx,
		clients:  clis,
		HTTPCase: fn(),
	}
	return c
}

// run tries to run the HTTP api bench.
func (c *httpController) run() {
	if c.GetQPS() <= 0 || c.cancel != nil {
		return
	}
	c.ctx, c.cancel = context.WithCancel(c.pctx)
	qps := c.GetQPS()
	burst := c.GetBurst()
	cliNum := int64(len(c.clients))
	tt := time.Duration(base*burst*cliNum/qps) * time.Microsecond
	log.Info("[api] begin to run http case", zap.String("case", c.Name()), zap.Int64("c.len", int64(len(c.clients))),
		zap.Int64("qps", qps), zap.Int64("burst", burst), zap.Duration("interval", tt))
	for _, hCli := range c.clients {
		c.wg.Add(1)
		go func(hCli pdHttp.Client) {
			defer c.wg.Done()
			c.wg.Add(int(burst))
			for i := int64(0); i < burst; i++ {
				go func() {
					defer c.wg.Done()
					var ticker = time.NewTicker(tt)
					defer ticker.Stop()
					for {
						select {
						case <-ticker.C:
							err := c.Do(c.ctx, hCli)
							if err != nil {
								log.Error("[api] meet error when doing HTTP request", zap.String("case", c.Name()), zap.Error(err))
							}
						case <-c.ctx.Done():
							log.Info("[api] Got signal to exit running HTTP case")
							return
						}
					}
				}()
			}
		}(hCli)
	}
}

// stop stops the HTTP api bench.
func (c *httpController) stop() {
	if c.cancel == nil {
		return
	}
	c.cancel()
	c.cancel = nil
	c.wg.Wait()
}

type gRPCController struct {
	GRPCCase
	clients []pd.Client
	pctx    context.Context

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup
}

func newGRPCController(ctx context.Context, clis []pd.Client, fn GRPCCreateFn) *gRPCController {
	c := &gRPCController{
		pctx:     ctx,
		clients:  clis,
		GRPCCase: fn(),
	}
	return c
}

// run tries to run the gRPC api bench.
func (c *gRPCController) run() {
	if c.GetQPS() <= 0 || c.cancel != nil {
		return
	}
	c.ctx, c.cancel = context.WithCancel(c.pctx)
	qps := c.GetQPS()
	burst := c.GetBurst()
	cliNum := int64(len(c.clients))
	tt := time.Duration(base*burst*cliNum/qps) * time.Microsecond
	log.Info("[api] begin to run gRPC case", zap.String("case", c.Name()), zap.Int64("cliens.len", int64(len(c.clients))),
		zap.Int64("qps", qps), zap.Int64("burst", burst), zap.Duration("interval", tt))
	for _, cli := range c.clients {
		c.wg.Add(1)
		go func(cli pd.Client) {
			defer c.wg.Done()
			c.wg.Add(int(burst))
			for i := int64(0); i < burst; i++ {
				go func() {
					defer c.wg.Done()
					var ticker = time.NewTicker(tt)
					defer ticker.Stop()
					for {
						select {
						case <-ticker.C:
							err := c.Unary(c.ctx, cli)
							if err != nil {
								log.Error("[api] meet error when doing gRPC request", zap.String("case", c.Name()), zap.Error(err))
							}
						case <-c.ctx.Done():
							log.Info("[api] Got signal to exit running gRPC case")
							return
						}
					}
				}()
			}
		}(cli)
	}
}

// stop stops the gRPC api bench.
func (c *gRPCController) stop() {
	if c.cancel == nil {
		return
	}
	c.cancel()
	c.cancel = nil
	c.wg.Wait()
}

type etcdController struct {
	ETCDCase
	clients []*clientv3.Client
	pctx    context.Context

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup
}

func newEtcdController(ctx context.Context, clis []*clientv3.Client, fn ETCDCreateFn) *etcdController {
	c := &etcdController{
		pctx:     ctx,
		clients:  clis,
		ETCDCase: fn(),
	}
	return c
}

// run tries to run the gRPC api bench.
func (c *etcdController) run() {
	if c.GetQPS() <= 0 || c.cancel != nil {
		return
	}
	c.ctx, c.cancel = context.WithCancel(c.pctx)
	qps := c.GetQPS()
	burst := c.GetBurst()
	cliNum := int64(len(c.clients))
	tt := time.Duration(base*burst*cliNum/qps) * time.Microsecond
	log.Info("[api] begin to run etcd case", zap.String("case", c.Name()), zap.Int64("qps", qps), zap.Int64("burst", burst), zap.Duration("interval", tt))
	err := c.Init(c.ctx, c.clients[0])
	if err != nil {
		log.Error("[api] init error", zap.String("case", c.Name()), zap.Error(err))
		return
	}
	for _, cli := range c.clients {
		c.wg.Add(1)
		go func(cli *clientv3.Client) {
			defer c.wg.Done()
			c.wg.Add(int(burst))
			for i := int64(0); i < burst; i++ {
				go func() {
					defer c.wg.Done()
					var ticker = time.NewTicker(tt)
					defer ticker.Stop()
					for {
						select {
						case <-ticker.C:
							err := c.Unary(c.ctx, cli)
							if err != nil {
								log.Error("[api] meet error when doing etcd request", zap.String("case", c.Name()), zap.Error(err))
							}
						case <-c.ctx.Done():
							log.Info("[api] Got signal to exit running etcd case")
							return
						}
					}
				}()
			}
		}(cli)
	}
}

// stop stops the etcd api bench.
func (c *etcdController) stop() {
	if c.cancel == nil {
		return
	}
	c.cancel()
	c.cancel = nil
	c.wg.Wait()
}
