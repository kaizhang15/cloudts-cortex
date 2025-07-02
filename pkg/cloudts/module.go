// pkg/cloudts/module.go
package cloudts

import (
	"context"
	"time"

	"github.com/cortexproject/cortex/pkg/util/modules"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/storage"
)

const (
	defaultPartitionDuration = 2 * time.Hour
	defaultMaxUploadConns    = 10
)

type Config struct {
	S3Config          S3Config
	PartitionDuration time.Duration `yaml:"partition_duration"`
	MaxUploadConns    int           `yaml:"max_upload_connections"`
	Logger            log.Logger    `yaml:"-"`
}

type Module struct {
	mgr    *CloudTSManager
	cfg    Config
	logger log.Logger
}

func New(cfg Config) (*Module, error) {
	// 设置默认值
	if cfg.PartitionDuration == 0 {
		cfg.PartitionDuration = defaultPartitionDuration
	}
	if cfg.MaxUploadConns == 0 {
		cfg.MaxUploadConns = defaultMaxUploadConns
	}
	if cfg.Logger == nil {
		cfg.Logger = log.NewNopLogger()
	}

	// 初始化管理器
	mgr := NewCloudTSManager(cfg.S3Config, cfg.Logger)

	return &Module{
		mgr:    mgr,
		cfg:    cfg,
		logger: cfg.Logger,
	}, nil
}

func (m *Module) Register() (modules.Module, error) {
	return modules.NewModule(
		"cloudts",
		m.start,
		m.stop,
	), nil
}

func (m *Module) start(ctx context.Context) error {
	// if err := m.mgr.Start(ctx); err != nil {
	// 	level.Error(m.logger).Log("msg", "failed to start CloudTS manager", "err", err)
	// 	return err
	// }
	level.Info(m.logger).Log("msg", "CloudTS module started")
	return nil
}

func (m *Module) stop() error {
	if err := m.mgr.Close(); err != nil {
		level.Error(m.logger).Log("msg", "failed to stop CloudTS manager", "err", err)
		return err
	}
	level.Info(m.logger).Log("msg", "CloudTS module stopped")
	return nil
}

func (m *Module) Storage() storage.Storage {
	return NewCortexStorageAdapter(m.mgr, m.logger)
}
