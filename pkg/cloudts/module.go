package cloudts

import (
	"context"
	"errors"
	"flag"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

type Config struct {
	S3Config          S3Config      `yaml:"s3"`
	PartitionDuration time.Duration `yaml:"partition_duration"`
	MaxUploadConns    int           `yaml:"max_upload_connections"`
	Logger            log.Logger    `yaml:"-"`

	// 新增服务相关配置
	// HealthCheckInterval   time.Duration `yaml:"health_check_interval"`
	// MetricsReportInterval time.Duration `yaml:"metrics_report_interval"`
}

type CloudTSModule struct {
	mgr       *CloudTSManager
	cfg       Config
	logger    log.Logger
	runCancel context.CancelFunc
	wg        sync.WaitGroup
}

const (
	defaultPartitionDuration   = 2 * time.Hour
	defaultMaxUploadConns      = 10
	defaultHealthCheckInterval = 5 * time.Minute
	defaultMetricsInterval     = 1 * time.Minute
)

func New(cfg Config) (*CloudTSModule, error) {
	// 设置默认值
	if cfg.PartitionDuration == 0 {
		cfg.PartitionDuration = defaultPartitionDuration
	}
	if cfg.MaxUploadConns == 0 {
		cfg.MaxUploadConns = defaultMaxUploadConns
	}
	// if cfg.HealthCheckInterval == 0 {
	// 	cfg.HealthCheckInterval = defaultHealthCheckInterval
	// }
	// if cfg.MetricsReportInterval == 0 {
	// 	cfg.MetricsReportInterval = defaultMetricsInterval
	// }
	if cfg.Logger == nil {
		cfg.Logger = log.NewNopLogger()
	}

	mgr := NewCloudTSManager(cfg.S3Config, cfg.Logger)
	return &CloudTSModule{
		mgr:    mgr,
		cfg:    cfg,
		logger: cfg.Logger,
	}, nil
}

func (m *CloudTSModule) Register() (services.Service, error) {
	return services.NewBasicService(
		m.start,
		m.run,
		m.stop,
	), nil
}

func (m *CloudTSModule) start(ctx context.Context) error {
	// // 1. 初始化云存储连接
	// if err := m.mgr.cloudWriter.Init(); err != nil {
	// 	return fmt.Errorf("cloud writer init failed: %w", err)
	// }

	// 2. 启动后台协程
	m.wg.Add(2)
	// go m.runMetricsLoop(ctx)
	// go m.runHealthCheckLoop(ctx)

	level.Info(m.logger).Log(
		"msg", "module started",
		"partition_duration", m.cfg.PartitionDuration,
		"upload_conns", m.cfg.MaxUploadConns,
	)
	return nil
}

func (m *CloudTSModule) run(ctx context.Context) error {
	// 创建可取消的context
	_, cancel := context.WithCancel(context.Background())
	m.runCancel = cancel
	defer cancel()

	// 主循环
	ticker := time.NewTicker(30)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 定期执行存储健康检查
			// if err := m.checkStorageHealth(runCtx); err != nil {
			// 	level.Warn(m.logger).Log("msg", "storage health check failed", "err", err)
			// 	// 可添加自动修复逻辑
			// }

		case <-ctx.Done():
			level.Debug(m.logger).Log("msg", "shutting down run loop")
			return nil
		}
	}
}
func (m *CloudTSModule) stop(_ error) error {
	// 1. 停止run循环
	if m.runCancel != nil {
		m.runCancel()
	}

	// 2. 等待后台协程退出
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		level.Info(m.logger).Log("msg", "all background tasks stopped")
	case <-time.After(30 * time.Second):
		level.Error(m.logger).Log("msg", "timeout waiting for graceful shutdown")
	}

	// 3. 关闭管理器
	if err := m.mgr.Close(); err != nil {
		level.Error(m.logger).Log("msg", "failed to close manager", "err", err)
		return err
	}

	level.Info(m.logger).Log("msg", "module fully stopped")
	return nil
}

// RegisterFlags 为配置添加命令行参数
func (cfg *Config) RegisterFlags(fs *flag.FlagSet) {
	fs.StringVar(&cfg.S3Config.Endpoint, "cloudts.s3.endpoint", "", "S3 endpoint URL")
	fs.StringVar(&cfg.S3Config.Bucket, "cloudts.s3.bucket", "", "S3 bucket name")
	fs.StringVar(&cfg.S3Config.Region, "cloudts.s3.region", "", "S3 region")
	fs.StringVar(&cfg.S3Config.AccessKey, "cloudts.s3.access_key", "", "S3 access_key")
	fs.StringVar(&cfg.S3Config.SecretKey, "cloudts.s3.secret_key", "", "S3 secret_key")
	fs.StringVar(&cfg.S3Config.SessionToken, "cloudts.s3.session_token", "", "S3 session token")
	fs.DurationVar(&cfg.PartitionDuration, "cloudts.partition-duration", 2*time.Hour, "Data partition duration")
	fs.IntVar(&cfg.MaxUploadConns, "cloudts.max-upload-conns", 10, "Maximum upload connections")
}

// Validate 配置校验
func (cfg *Config) Validate() error {
	if cfg.S3Config.Bucket == "" {
		return errors.New("cloudts.s3.bucket must be specified")
	}
	return nil
}

// func (m *Module) runMetricsLoop(ctx context.Context) {
// 	defer m.wg.Done()

// 	ticker := time.NewTicker(m.cfg.MetricsReportInterval)
// 	defer ticker.Stop()

// 	for {
// 		select {
// 		case <-ticker.C:
// 			stats := m.mgr.GetStats()
// 			level.Debug(m.logger).Log(
// 				"msg", "storage metrics",
// 				"partitions", stats.PartitionCount,
// 				"objects", stats.TSObjectCount,
// 				"earliest_ts", time.UnixMilli(stats.EarliestTimestamp).Format(time.RFC3339),
// 			)

// 		case <-ctx.Done():
// 			level.Debug(m.logger).Log("msg", "metrics loop exiting")
// 			return
// 		}
// 	}
// }

// func (m *Module) runHealthCheckLoop(ctx context.Context) {
// 	defer m.wg.Done()

// 	ticker := time.NewTicker(m.cfg.HealthCheckInterval / 2) // 更频繁的检查
// 	defer ticker.Stop()

// 	for {
// 		select {
// 		case <-ticker.C:
// 			if err := m.checkStorageHealth(ctx); err != nil {
// 				m.handleHealthFailure(err)
// 			}

// 		case <-ctx.Done():
// 			level.Debug(m.logger).Log("msg", "health check loop exiting")
// 			return
// 		}
// 	}
// }

// func (m *Module) checkStorageHealth(ctx context.Context) error {
// 	// 示例检查项：
// 	// 1. 验证最早时间戳是否有效
// 	if ts, err := m.mgr.GetEarliestTimestamp(); err != nil {
// 		return fmt.Errorf("earliest timestamp check failed: %w", err)
// 	} else if time.Since(time.UnixMilli(ts)) > 30*24*time.Hour {
// 		return errors.New("stale data detected (over 30 days old)")
// 	}

// 	// 2. 检查分区健康状态
// 	if active := len(m.mgr.partitions); active == 0 {
// 		return errors.New("no active partitions")
// 	}

// 	return nil
// }

// s3_config:
//   endpoint: "s3.amazonaws.com"
//   bucket: "my-tsdb"
//   access_key: "AKIA..."
//   secret_key: "secret"

// partition_duration: "2h"  # 分区时间窗口
// max_upload_connections: 15

// health_check_interval: "3m"
// metrics_report_interval: "30s"

// # cortex/config.yaml
// cloudts:
//   enabled: true
//   s3_config:
//     endpoint: "s3.amazonaws.com"
//     bucket: "my-tsdb"
//     access_key: "AKIA..."
//     secret_key: "secret"
//   partition_duration: "2h"
//   max_upload_connections: 15
//   health_check_interval: "3m"
//   metrics_report_interval: "30s"
