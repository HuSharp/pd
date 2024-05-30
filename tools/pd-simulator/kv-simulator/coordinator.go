package simulator

import (
	"context"
	"fmt"
	"github.com/tikv/pd/pkg/utils/logutil"
	"os"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/utils/jsonutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	svrCfg "github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tools/pd-dev/pd-analysis/analysis"
	"github.com/tikv/pd/tools/pd-simulator/kv-simulator/simulator"
	"github.com/tikv/pd/tools/pd-simulator/kv-simulator/simulator/cases"
	sc "github.com/tikv/pd/tools/pd-simulator/kv-simulator/simulator/config"
	"go.uber.org/zap"
)

type Coordinator struct {
	pCtx   context.Context
	ctx    context.Context
	cancel context.CancelFunc

	cfg *sc.SimConfig
	wg  *sync.WaitGroup

	er *simulator.EventRunner
}

func newCoordinator(ctx context.Context, cfg *sc.SimConfig) *Coordinator {
	cfg.Mode = "kv-simulator"
	log.Info("[kv-simulator] create coordinator", zap.Any("config", cfg))
	return &Coordinator{
		pCtx: ctx,
		cfg:  cfg,
		wg:   &sync.WaitGroup{},
	}
}

func (c *Coordinator) Run(name string) {
	if c.cancel != nil {
		log.Error("[kv-simulator] coordinator is already running")
		return
	}
	c.ctx, c.cancel = context.WithCancel(c.pCtx)
	log.Info("[kv-simulator] coordinator start")
	//simutil.InitLogger(c.cfg.SimLogLevel, c.cfg.Log.File.Filename)
	statistics.Denoising = false

	schedulers.Register() // register schedulers, which is needed by simConfig.Adjust
	if c.cfg.EnableTransferRegionCounter {
		analysis.GetTransferCounter().Init(c.cfg.TotalStore, c.cfg.TotalRegion)
	}

	if len(c.cfg.CaseName) == 0 {
		if name != "" {
			c.cfg.CaseName = name
		} else {
			c.cfg.CaseName = name
		}
	}

	if c.cfg.CaseName == "" {
		for simCase := range cases.CaseMap {
			c.simRun(simCase)
		}
	} else {
		c.simRun(c.cfg.CaseName)
	}
}

func (c *Coordinator) Stop() {
	if c.cancel == nil {
		return
	}
	c.cancel()
	c.cancel = nil
	c.wg.Wait()
	if c.cfg.CaseName != "" {
		log.Info("[kv-simulator] coordinator stopped", zap.String("case", c.cfg.CaseName))
	} else {
		log.Info("[kv-simulator] coordinator stopped ALL cases")
	}
}

func (c *Coordinator) GetConfig() *sc.SimConfig {
	return c.cfg
}

func (c *Coordinator) GetEventRunner() *simulator.EventRunner {
	return c.er
}

func (c *Coordinator) simRun(simCase string) {
	if c.cfg.PDAddrs != "" {
		log.Info("[kv-simulator] start simulator", zap.String("case", simCase))
		c.simStart(c.cfg.PDAddrs, simCase)
	} else {
		log.Info("[kv-simulator] start simulator with new PD", zap.String("case", simCase), zap.Any("config", c.cfg))
		local, clean := NewSingleServer(context.Background(), c.cfg)
		err := local.Run()
		if err != nil {
			log.Fatal("[kv-simulator] run server error", zap.Error(err))
		}
		for {
			if !local.IsClosed() && local.GetMember().IsLeader() {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		c.simStart(local.GetAddr(), simCase, clean)
	}
}

func (c *Coordinator) UpdateConfig(oldConfig *sc.SimConfig, key string, value any) error {
	updated, found, err := jsonutil.AddKeyValue(&oldConfig, key, value)
	if err != nil {
		return err
	}

	if !found {
		return errors.Errorf("[kv-simulator] config item %s not found", key)
	}

	if updated {
		*c.cfg = *oldConfig
	}
	return err
}

// NewSingleServer creates a pd server for simulator.
func NewSingleServer(ctx context.Context, simConfig *sc.SimConfig) (*server.Server, testutil.CleanupFunc) {
	err := logutil.SetupLogger(simConfig.ServerConfig.Log, &simConfig.ServerConfig.Logger, &simConfig.ServerConfig.LogProps)
	if err != nil {
		log.Fatal("[kv-simulator] setup logger error", zap.Error(err))
	}

	s, err := server.CreateServer(ctx, simConfig.ServerConfig, nil, api.NewHandler)
	if err != nil {
		panic("[kv-simulator] create server failed")
	}

	cleanup := func() {
		s.Close()
		cleanServer(simConfig.ServerConfig)
	}
	return s, cleanup
}

func cleanServer(cfg *svrCfg.Config) {
	// Clean data directory
	os.RemoveAll(cfg.DataDir)
}

var driver *simulator.Driver

func (c *Coordinator) GetCase() string {
	return driver.GetSimCase()
}

func (c *Coordinator) UpdateDriver(name string) error {
	return driver.UpdateSimCase(name)
}

func (c *Coordinator) simStart(pdAddr string, simCase string, clean ...testutil.CleanupFunc) {
	start := time.Now()
	var err error
	driver, err = simulator.NewDriver(pdAddr, simCase, c.cfg)
	if err != nil {
		log.Fatal("[kv-simulator] create driver error", zap.Error(err))
	}

	err = driver.Prepare()
	if err != nil {
		log.Fatal("[kv-simulator] prepare error", zap.Error(err))
	}
	// Set the eventRunner to coordinator
	c.er = driver.GetEventRunner()
	tickInterval := c.cfg.SimTickInterval.Duration

	tick := time.NewTicker(tickInterval)
	defer tick.Stop()

	simResult := "FAIL"

EXIT:
	for {
		select {
		case <-tick.C:
			driver.Tick()
			if driver.Check() {
				simResult = "OK"
				break EXIT
			}
		case <-c.ctx.Done():
			break EXIT
		}
	}

	driver.Stop()
	if len(clean) != 0 && clean[0] != nil {
		clean[0]()
	}

	log.Info(fmt.Sprintf("[kv-simulator] %s [%s] total iteration: %d, time cost: %v\n", simResult, simCase, driver.TickCount(), time.Since(start)))
	if analysis.GetTransferCounter().IsValid {
		analysis.GetTransferCounter().PrintResult()
	}

	if simResult != "OK" {
		return
	}
}
