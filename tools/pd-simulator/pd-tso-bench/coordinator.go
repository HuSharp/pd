package tsobench

import (
	"context"
	"fmt"
	"github.com/influxdata/tdigest"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/pkg/utils/jsonutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"math/rand"
	"net/http/httptest"
	"sync"
	"time"
)

type Coordinator struct {
	pCtx   context.Context
	ctx    context.Context
	cancel context.CancelFunc
	cfg    *Config

	wg *sync.WaitGroup
}

func newCoordinator(ctx context.Context, cfg *Config) *Coordinator {
	return &Coordinator{
		pCtx: ctx,
		cfg:  cfg,
		wg:   &sync.WaitGroup{},
	}
}

func (c *Coordinator) Run() {
	if c.cancel != nil {
		log.Info("[tso] coordinator is already running")
		return
	}
	log.Info("[tso] start to run coordinator")
	c.ctx, c.cancel = context.WithCancel(c.pCtx)
	for i := 0; i < c.cfg.Count; i++ {
		log.Info(fmt.Sprintf("[tso] Sart benchmark #%d, duration: %+vs", i, c.cfg.Duration.Seconds()))
		c.bench()
	}
}

var promServer *httptest.Server

func (c *Coordinator) bench() {
	promServer = httptest.NewServer(promhttp.Handler())

	// Initialize all clients
	log.Info(fmt.Sprintf("Create %d client(s) for benchmark", c.cfg.Client))
	pdClients := make([]pd.Client, c.cfg.Client)
	for idx := range pdClients {
		pdCli, err := createPDClient(c.ctx, c.cfg)
		if err != nil {
			log.Fatal(fmt.Sprintf("[tso] create pd client #%d failed: %v", idx, err))
		}
		pdClients[idx] = pdCli
	}

	ctx, cancel := context.WithCancel(c.ctx)
	// To avoid the first time high latency.
	for idx, pdCli := range pdClients {
		_, _, err := pdCli.GetLocalTS(ctx, c.cfg.DcLocation)
		if err != nil {
			log.Fatal("[tso] get first time tso failed", zap.Int("client-number", idx), zap.Error(err))
		}
	}

	durCh := make(chan time.Duration, 2*(c.cfg.Concurrency)*(c.cfg.Client))

	if c.cfg.EnableFaultInjection {
		log.Info(fmt.Sprintf("[tso] Enable fault injection, failure rate: %f", c.cfg.FaultInjectionRate))
		c.wg.Add(c.cfg.Client)
		for i := 0; i < c.cfg.Client; i++ {
			go c.reqWorker(ctx, c.cfg, pdClients, i, durCh)
		}
	} else {
		c.wg.Add(c.cfg.Concurrency * c.cfg.Client)
		for i := 0; i < c.cfg.Client; i++ {
			for j := 0; j < c.cfg.Concurrency; j++ {
				go c.reqWorker(ctx, c.cfg, pdClients, i, durCh)
			}
		}
	}

	c.wg.Add(1)
	go c.showStats(ctx, c.cfg.Interval, c.cfg.Verbose, durCh)

	timer := time.NewTimer(c.cfg.Duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-c.pCtx.Done():
	case <-timer.C:
	}
	cancel()

	c.wg.Wait()
	for _, pdCli := range pdClients {
		pdCli.Close()
	}
}

func (c *Coordinator) GetConfig() *Config {
	return c.cfg
}

func (c *Coordinator) UpdateConfig(oldConfig *Config, key string, value any) error {
	updated, found, err := jsonutil.AddKeyValue(&oldConfig, key, value)
	if err != nil {
		return err
	}

	if !found {
		return errors.Errorf("config item %s not found", key)
	}

	if updated {
		*c.cfg = *oldConfig
	}
	return err
}

var latencyTDigest = tdigest.New()

func (c *Coordinator) reqWorker(ctx context.Context, cfg *Config,
	pdClients []pd.Client, clientIdx int, durCh chan time.Duration) {
	defer c.wg.Done()

	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		err                    error
		maxRetryTime           = 120
		sleepIntervalOnFailure = 1000 * time.Millisecond
		totalSleepBeforeGetTS  time.Duration
	)
	pdCli := pdClients[clientIdx]

	for {
		if pdCli == nil || (cfg.EnableFaultInjection && shouldInjectFault(cfg)) {
			if pdCli != nil {
				pdCli.Close()
			}
			pdCli, err = createPDClient(ctx, cfg)
			if err != nil {
				log.Error(fmt.Sprintf("[tso] re-create pd client #%d failed: %v", clientIdx, err))
				select {
				case <-reqCtx.Done():
				case <-time.After(100 * time.Millisecond):
				}
				continue
			}
			pdClients[clientIdx] = pdCli
		}

		totalSleepBeforeGetTS = 0
		start := time.Now()

		i := 0
		for ; i < maxRetryTime; i++ {
			ticker := time.NewTicker(1)
			if cfg.MaxTSOSendIntervalMilliseconds > 0 {
				sleepBeforeGetTS := time.Duration(rand.Intn(cfg.MaxTSOSendIntervalMilliseconds)) * time.Millisecond
				ticker = time.NewTicker(sleepBeforeGetTS)
				select {
				case <-reqCtx.Done():
				case <-ticker.C:
					totalSleepBeforeGetTS += sleepBeforeGetTS
				}
			}
			_, _, err = pdCli.GetLocalTS(reqCtx, cfg.DcLocation)
			if errors.Cause(err) == context.Canceled {
				ticker.Stop()
				return
			}
			if err == nil {
				ticker.Stop()
				break
			}
			log.Error(fmt.Sprintf("%v", err))
			time.Sleep(sleepIntervalOnFailure)
		}
		if err != nil {
			log.Fatal(fmt.Sprintf("%v", err))
		}
		dur := time.Since(start) - time.Duration(i)*sleepIntervalOnFailure - totalSleepBeforeGetTS

		select {
		case <-reqCtx.Done():
			return
		case durCh <- dur:
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

func createPDClient(ctx context.Context, cfg *Config) (pd.Client, error) {
	var (
		pdCli pd.Client
		err   error
	)

	opts := make([]pd.ClientOption, 0)
	opts = append(opts, pd.WithGRPCDialOptions(
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    keepaliveTime,
			Timeout: keepaliveTimeout,
		}),
	))

	if len(cfg.KeyspaceName) > 0 {
		apiCtx := pd.NewAPIContextV2(cfg.KeyspaceName)
		pdCli, err = pd.NewClientWithAPIContext(ctx, apiCtx, []string{cfg.PDAddrs}, pd.SecurityOption{
			CAPath:   cfg.CaPath,
			CertPath: cfg.CertPath,
			KeyPath:  cfg.KeyPath,
		}, opts...)
	} else {
		pdCli, err = pd.NewClientWithKeyspace(ctx, uint32(cfg.KeyspaceID), []string{cfg.PDAddrs}, pd.SecurityOption{
			CAPath:   cfg.CaPath,
			CertPath: cfg.CertPath,
			KeyPath:  cfg.KeyPath,
		}, opts...)
	}
	if err != nil {
		return nil, err
	}

	pdCli.UpdateOption(pd.MaxTSOBatchWaitInterval, cfg.MaxBatchWaitInterval)
	pdCli.UpdateOption(pd.EnableTSOFollowerProxy, cfg.EnableTSOFollowerProxy)
	return pdCli, err
}

func shouldInjectFault(cfg *Config) bool {
	return rand.Intn(10000) < int(cfg.FaultInjectionRate*10000)
}

func (c *Coordinator) showStats(ctx context.Context, interval time.Duration, verbose bool, durCh chan time.Duration) {
	defer c.wg.Done()

	statCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	s := newStats()
	total := newStats()

	for {
		select {
		case <-ticker.C:
			// runtime.GC()
			if verbose {
				log.Info(fmt.Sprintf("[tso] %s", s.Counter()))
			}
			total.merge(s)
			s = newStats()
		case d := <-durCh:
			s.update(d)
		case <-statCtx.Done():
			log.Info(fmt.Sprintf("[tso] Total:"))
			log.Info(fmt.Sprintf("%s", total.Counter()))
			log.Info(fmt.Sprintf("%s", total.Percentage()))
			// Calculate the percentiles by using the tDigest algorithm.
			log.Info(fmt.Sprintf("P0.5: %.4fms, P0.8: %.4fms, P0.9: %.4fms, P0.99: %.4fms",
				latencyTDigest.Quantile(0.5), latencyTDigest.Quantile(0.8), latencyTDigest.Quantile(0.9), latencyTDigest.Quantile(0.99)))
			if verbose {
				log.Info(fmt.Sprintf("[tso] %s", collectMetrics(promServer)))
			}
			return
		}
	}
}
