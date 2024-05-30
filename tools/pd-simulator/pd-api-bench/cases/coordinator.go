package cases

import (
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	pdHttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/tlsutil"
	"github.com/tikv/pd/tools/pd-simulator/pd-api-bench/config"
	"github.com/tikv/pd/tools/pd-simulator/pd-api-bench/metrics"
	"github.com/tikv/pd/tools/pd-simulator/util"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Coordinator managers the operation of the gRPC and HTTP case.
type Coordinator struct {
	ctx context.Context
	cfg *config.Config

	httpClients []pdHttp.Client
	gRPCClients []pd.Client
	etcdClients []*clientv3.Client

	http map[string]*httpController
	grpc map[string]*gRPCController
	etcd map[string]*etcdController

	mu sync.RWMutex
}

// NewCoordinator returns a new coordinator.
func NewCoordinator(ctx context.Context, cfg *config.Config) *Coordinator {
	cfg.Mode = "api"
	return &Coordinator{
		ctx:  ctx,
		cfg:  cfg,
		http: make(map[string]*httpController),
		grpc: make(map[string]*gRPCController),
		etcd: make(map[string]*etcdController),
	}
}

// Run runs the coordinator.
func (c *Coordinator) Run() {
	// set client
	if c.cfg.Client == 0 {
		log.Error("[api] concurrency == 0, please input api.client greater than 0, exit")
		return
	}
	pdClis := make([]pd.Client, c.cfg.Client)
	for i := int64(0); i < c.cfg.Client; i++ {
		pdClis[i] = newPDClient(c.ctx, c.cfg)
		pdClis[i].UpdateOption(pd.EnableFollowerHandle, true)
	}
	etcdClis := make([]*clientv3.Client, c.cfg.Client)
	for i := int64(0); i < c.cfg.Client; i++ {
		etcdClis[i] = newEtcdClient(c.cfg)
	}
	httpClis := make([]pdHttp.Client, c.cfg.Client)
	for i := int64(0); i < c.cfg.Client; i++ {
		sd := pdClis[i].GetServiceDiscovery()
		httpClis[i] = pdHttp.NewClientWithServiceDiscovery("tools-api-bench", sd,
			pdHttp.WithTLSConfig(util.LoadTLSConfig(c.cfg.GeneralConfig)),
			pdHttp.WithMetrics(metrics.PDAPIRequestCounter, metrics.PDAPIExecutionHistogram))
	}
	c.httpClients = httpClis
	c.gRPCClients = pdClis
	c.etcdClients = etcdClis
	err := InitCluster(c.ctx, pdClis[0], httpClis[0])
	if err != nil {
		log.Fatal("[api] InitCluster error", zap.Error(err))
	}

	hcaseStr := strings.Split(c.cfg.HTTPCases, ",")
	for _, str := range hcaseStr {
		name, cfg := parseCaseNameAndConfig(c.cfg.QPS, c.cfg.Burst, str)
		if len(name) == 0 {
			continue
		}
		c.SetHTTPCase(name, cfg)
	}
	gcaseStr := strings.Split(c.cfg.GRPCCases, ",")
	for _, str := range gcaseStr {
		name, cfg := parseCaseNameAndConfig(c.cfg.QPS, c.cfg.Burst, str)
		if len(name) == 0 {
			continue
		}
		c.SetGRPCCase(name, cfg)
	}

	for name, cfg := range c.cfg.HTTP {
		err := c.SetHTTPCase(name, &cfg)
		if err != nil {
			log.Error("[api] create HTTP case failed", zap.Error(err))
		}
	}
	for name, cfg := range c.cfg.GRPC {
		err := c.SetGRPCCase(name, &cfg)
		if err != nil {
			log.Error("[api] create gRPC case failed", zap.Error(err))
		}
	}
	for name, cfg := range c.cfg.ETCD {
		err := c.SetETCDCase(name, &cfg)
		if err != nil {
			log.Error("[api] create etcd case failed", zap.Error(err))
		}
	}
}

func (c *Coordinator) Stop() {
	for _, controller := range c.http {
		controller.stop()
	}
	for _, controller := range c.grpc {
		controller.stop()
	}
	for _, controller := range c.etcd {
		controller.stop()
	}
}

func (c *Coordinator) GetConfig() *config.Config {
	return c.cfg
}

const (
	keepaliveTime    = 10 * time.Second
	keepaliveTimeout = 3 * time.Second
)

func newEtcdClient(cfg *config.Config) *clientv3.Client {
	lgc := zap.NewProductionConfig()
	lgc.Encoding = log.ZapEncodingName
	tlsCfg, err := tlsutil.TLSConfig{
		CAPath:   cfg.CaPath,
		CertPath: cfg.CertPath,
		KeyPath:  cfg.KeyPath,
	}.ToTLSConfig()
	if err != nil {
		log.Fatal("[api] fail to create etcd client", zap.Error(err))
	}
	clientConfig := clientv3.Config{
		Endpoints:   []string{cfg.PDAddrs},
		DialTimeout: keepaliveTimeout,
		TLS:         tlsCfg,
		LogConfig:   &lgc,
	}
	client, err := clientv3.New(clientConfig)
	if err != nil {
		log.Fatal("[api] fail to create pd client", zap.Error(err))
	}
	return client
}

// newPDClient returns a pd client.
func newPDClient(ctx context.Context, cfg *config.Config) pd.Client {
	addrs := []string{cfg.PDAddrs}
	pdCli, err := pd.NewClientWithContext(ctx, addrs, pd.SecurityOption{
		CAPath:   cfg.CaPath,
		CertPath: cfg.CertPath,
		KeyPath:  cfg.KeyPath,
	},
		pd.WithGRPCDialOptions(
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:    keepaliveTime,
				Timeout: keepaliveTimeout,
			}),
		))
	if err != nil {
		log.Fatal("[api] fail to create pd client", zap.Error(err))
	}
	return pdCli
}

func parseCaseNameAndConfig(qps int64, burst int64, str string) (string, *config.CaseConfig) {
	var err error
	cfg := &config.CaseConfig{}
	name := ""
	strs := strings.Split(str, "-")
	// to get case name
	strsa := strings.Split(strs[0], "+")
	name = strsa[0]
	// to get case Burst
	if len(strsa) > 1 {
		cfg.Burst, err = strconv.ParseInt(strsa[1], 10, 64)
		if err != nil {
			log.Error("[api] parse burst failed for case", zap.String("case", name), zap.String("config", strsa[1]))
		}
	}
	// to get case qps
	if len(strs) > 1 {
		strsb := strings.Split(strs[1], "+")
		cfg.QPS, err = strconv.ParseInt(strsb[0], 10, 64)
		if err != nil {
			if err != nil {
				log.Error("[api] parse qps failed for case", zap.String("case", name), zap.String("config", strsb[0]))
			}
		}
		// to get case Burst
		if len(strsb) > 1 {
			cfg.Burst, err = strconv.ParseInt(strsb[1], 10, 64)
			if err != nil {
				log.Error("[api] parse burst failed for case", zap.String("case", name), zap.String("config", strsb[1]))
			}
		}
	}
	if cfg.QPS == 0 && qps > 0 {
		cfg.QPS = qps
	}
	if cfg.Burst == 0 && burst > 0 {
		cfg.Burst = burst
	}
	return name, cfg
}

// GetHTTPCase returns the HTTP case config.
func (c *Coordinator) GetHTTPCase(name string) (*config.CaseConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if controller, ok := c.http[name]; ok {
		return controller.GetConfig(), nil
	}
	return nil, errors.Errorf("[api] case %v does not exist", name)
}

// GetGRPCCase returns the gRPC case config.
func (c *Coordinator) GetGRPCCase(name string) (*config.CaseConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if controller, ok := c.grpc[name]; ok {
		return controller.GetConfig(), nil
	}
	return nil, errors.Errorf("[api] case %v does not exist", name)
}

// GetETCDCase returns the etcd case config.
func (c *Coordinator) GetETCDCase(name string) (*config.CaseConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if controller, ok := c.etcd[name]; ok {
		return controller.GetConfig(), nil
	}
	return nil, errors.Errorf("[api] case %v does not exist", name)
}

// GetAllHTTPCases returns the all HTTP case configs.
func (c *Coordinator) GetAllHTTPCases() map[string]*config.CaseConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ret := make(map[string]*config.CaseConfig)
	for name, c := range c.http {
		ret[name] = c.GetConfig()
	}
	return ret
}

// GetAllGRPCCases returns the all gRPC case configs.
func (c *Coordinator) GetAllGRPCCases() map[string]*config.CaseConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ret := make(map[string]*config.CaseConfig)
	for name, c := range c.grpc {
		ret[name] = c.GetConfig()
	}
	return ret
}

// GetAllETCDCases returns the all etcd case configs.
func (c *Coordinator) GetAllETCDCases() map[string]*config.CaseConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ret := make(map[string]*config.CaseConfig)
	for name, c := range c.etcd {
		ret[name] = c.GetConfig()
	}
	return ret
}

// SetHTTPCase sets the config for the specific case.
func (c *Coordinator) SetHTTPCase(name string, cfg *config.CaseConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if fn, ok := HTTPCaseFnMap[name]; ok {
		var controller *httpController
		if controller, ok = c.http[name]; !ok {
			controller = newHTTPController(c.ctx, c.httpClients, fn)
			c.http[name] = controller
		}
		controller.stop()
		controller.SetQPS(cfg.QPS)
		if cfg.Burst > 0 {
			controller.SetBurst(cfg.Burst)
		}
		c.cfg.HTTP[name] = *cfg.Clone()
		controller.run()
	} else {
		return errors.Errorf("[api] HTTP case %s not implemented", name)
	}
	return nil
}

// SetGRPCCase sets the config for the specific case.
func (c *Coordinator) SetGRPCCase(name string, cfg *config.CaseConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if fn, ok := GRPCCaseFnMap[name]; ok {
		var controller *gRPCController
		if controller, ok = c.grpc[name]; !ok {
			controller = newGRPCController(c.ctx, c.gRPCClients, fn)
			c.grpc[name] = controller
		}
		controller.stop()
		controller.SetQPS(cfg.QPS)
		if cfg.Burst > 0 {
			controller.SetBurst(cfg.Burst)
		}
		c.cfg.GRPC[name] = *cfg.Clone()
		controller.run()
	} else {
		return errors.Errorf("[api] gRPC case %s not implemented", name)
	}
	return nil
}

// SetETCDCase sets the config for the specific case.
func (c *Coordinator) SetETCDCase(name string, cfg *config.CaseConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if fn, ok := ETCDCaseFnMap[name]; ok {
		var controller *etcdController
		if controller, ok = c.etcd[name]; !ok {
			controller = newEtcdController(c.ctx, c.etcdClients, fn)
			c.etcd[name] = controller
		}
		controller.stop()
		controller.SetQPS(cfg.QPS)
		if cfg.Burst > 0 {
			controller.SetBurst(cfg.Burst)
		}
		c.cfg.ETCD[name] = *cfg.Clone()
		controller.run()
	} else {
		return errors.Errorf("[api] etcd case %s not implemented", name)
	}
	return nil
}

func (c *Coordinator) DeleteHTTPCase(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if controller, ok := c.http[name]; ok {
		controller.stop()
		delete(c.http, name)
		return nil
	}
	return errors.Errorf("[api] case %v does not exist", name)
}

func (c *Coordinator) DeleteGRPCCase(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if controller, ok := c.grpc[name]; ok {
		controller.stop()
		delete(c.grpc, name)
		return nil
	}
	return errors.Errorf("[api] case %v does not exist", name)
}

func (c *Coordinator) DeleteETCDCase(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if controller, ok := c.etcd[name]; ok {
		controller.stop()
		delete(c.etcd, name)
		return nil
	}
	return errors.Errorf("[api] case %v does not exist", name)
}
