package heartbeatbench

import (
	"context"
	"fmt"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	pdHttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/tools/pd-simulator/util"
	"go.uber.org/zap"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	pCtx   context.Context
	ctx    context.Context
	cancel context.CancelFunc
	Config *Config

	wg *sync.WaitGroup
}

func newCoordinator(ctx context.Context, cfg *Config) *Coordinator {
	cfg.Mode = "heartbeat"
	return &Coordinator{
		pCtx:   ctx,
		Config: cfg,
		wg:     &sync.WaitGroup{},
	}
}

func (c *Coordinator) Run() {
	if c.cancel != nil {
		log.Error("[heartbeat] coordinator is already running")
		return
	}
	log.Info("[heartbeat] start to run coordinator")
	c.ctx, c.cancel = context.WithCancel(c.pCtx)
	cli, err := newClient(c.ctx, c.Config)
	if err != nil {
		log.Fatal("[heartbeat] create client error", zap.Error(err))
	}

	initClusterID(c.ctx, cli)
	regions := new(Regions)
	regions.init(c.Config)
	log.Info("[heartbeat] finish init regions")
	stores := newStores(c.Config.StoreCount)
	stores.update(regions)
	bootstrap(c.ctx, cli)
	putStores(c.ctx, c.Config, cli, stores)
	log.Info("[heartbeat] finish put stores")
	clis := make(map[uint64]pdpb.PDClient, c.Config.StoreCount)
	httpCli := pdHttp.NewClient("tools-heartbeat-bench", []string{c.Config.PDAddrs}, pdHttp.WithTLSConfig(util.LoadTLSConfig(c.Config.GeneralConfig)))
	go deleteOperators(c.ctx, httpCli)
	streams := make(map[uint64]pdpb.PD_RegionHeartbeatClient, c.Config.StoreCount)
	for i := 1; i <= c.Config.StoreCount; i++ {
		clis[uint64(i)], streams[uint64(i)] = createHeartbeatStream(c.ctx, c.Config)
	}
	header := &pdpb.RequestHeader{
		ClusterId: clusterID,
	}
	var heartbeatTicker = time.NewTicker(regionReportInterval * time.Second)
	defer heartbeatTicker.Stop()
	var resolvedTSTicker = time.NewTicker(time.Second)
	defer resolvedTSTicker.Stop()
	for {
		select {
		case <-heartbeatTicker.C:
			if c.Config.Round != 0 && regions.updateRound > c.Config.Round {
				os.Exit(0)
			}
			rep := newReport(c.Config)
			r := rep.Stats()

			startTime := time.Now()
			for i := 1; i <= c.Config.StoreCount; i++ {
				id := uint64(i)
				c.wg.Add(1)
				go regions.handleRegionHeartbeat(c.wg, streams[id], id, rep)
			}
			c.wg.Wait()

			since := time.Since(startTime).Seconds()
			close(rep.Results())
			regions.result(c.Config.RegionCount, since)
			stats := <-r
			log.Info("[heartbeat] region heartbeat stats", zap.String("total", fmt.Sprintf("%.4fs", stats.Total.Seconds())),
				zap.String("slowest", fmt.Sprintf("%.4fs", stats.Slowest)),
				zap.String("fastest", fmt.Sprintf("%.4fs", stats.Fastest)),
				zap.String("average", fmt.Sprintf("%.4fs", stats.Average)),
				zap.String("stddev", fmt.Sprintf("%.4fs", stats.Stddev)),
				zap.String("rps", fmt.Sprintf("%.4f", stats.RPS)),
				zap.Uint64("max-epoch-version", maxVersion),
			)
			log.Info("[heartbeat] store heartbeat stats", zap.String("max", fmt.Sprintf("%.4fs", since)))
			options := newOptions(c.Config)
			regions.update(c.Config, options)
			go stores.update(regions) // update stores in background, unusually region heartbeat is slower than store update.
		case <-resolvedTSTicker.C:
			for i := 1; i <= c.Config.StoreCount; i++ {
				id := uint64(i)
				c.wg.Add(1)
				go func(wg *sync.WaitGroup, id uint64) {
					defer wg.Done()
					cli := clis[id]
					_, err := cli.ReportMinResolvedTS(c.ctx, &pdpb.ReportMinResolvedTsRequest{
						Header:        header,
						StoreId:       id,
						MinResolvedTs: uint64(time.Now().Unix()),
					})
					if err != nil {
						log.Error("[heartbeat] send resolved TS error", zap.Uint64("store-id", id), zap.Error(err))
						return
					}
				}(c.wg, id)
			}
			c.wg.Wait()
		case <-c.ctx.Done():
			log.Info("[heartbeat] got signal to exit")
			return
		case <-c.pCtx.Done():
			log.Info("[heartbeat] pd-simulator context done")
			return
		}
	}
}

func (c *Coordinator) Stop() {
	if c.cancel == nil {
		return
	}
	c.cancel()
	c.cancel = nil
	c.wg.Wait()
}

func (c *Coordinator) GetConfig() *Config {
	return c.Config
}
