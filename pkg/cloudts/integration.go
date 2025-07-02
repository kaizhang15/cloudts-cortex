// pkg/cloudts/integration.go
package cloudts

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/util/modules"
	"github.com/cortexproject/cortex/pkg/util/services"
	// "github.com/kaizhang15/cloudts-cortex/pkg/util/modules"
	// "github.com/kaizhang15/cloudts-cortex/pkg/util/modules"
)

const CloudTSModuleName = "cloudts"

type CortexAdapter struct {
	*CloudTSModule
	listeners    []services.Listener
	mtx          sync.Mutex
	state        services.State
	failureError error // 存储失败时的错误信息
}

var _ services.Service = (*CortexAdapter)(nil)

func RegisterModule(m *modules.Manager, cfg *Config) {
	fmt.Errorf("in integration registermodule\n")
	m.RegisterModule(CloudTSModuleName, func() (services.Service, error) {
		if err := cfg.Validate(); err != nil {
			return nil, fmt.Errorf("invalid cloudts config: %w", err)
		}
		return NewCortexAdapter(*cfg)
	})
}

// // 注册为Cortex模块
// func init() {
// 	modules.DefaultModuleManager.RegisterModule("cloudts",
// 		modules.ModuleInitFunc(func(cfg interface{}) (services.Service, error) {
// 			// 类型转换检查
// 			cloudtsCfg, ok := cfg.(Config)
// 			if !ok {
// 				return nil, fmt.Errorf("expected cloudts.Config, got %T", cfg)
// 			}

// 			return newCortexAdapter(cloudtsCfg)
// 		}),
// 	)
// }

func (a *CortexAdapter) ServiceName() string {
	return "cloudts"
}

func NewCortexAdapter(cfg Config) (*CortexAdapter, error) {
	mod, err := New(cfg)
	if err != nil {
		return nil, err
	}

	return &CortexAdapter{
		CloudTSModule: mod,
		state:         services.New,
		listeners:     make([]services.Listener, 0),
		mtx:           sync.Mutex{},
		failureError:  nil,
	}, nil
}

func (a *CortexAdapter) transitionState(newState services.State, failure error) {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	oldState := a.state
	a.state = newState
	a.failureError = failure // 存储单个错误

	for _, listener := range a.listeners {
		switch newState {
		case services.Starting:
			listener.Starting()
		case services.Running:
			listener.Running()
		case services.Stopping:
			listener.Stopping(oldState)
		case services.Terminated:
			listener.Terminated(oldState)
		case services.Failed:
			listener.Failed(oldState, failure)
		}
	}
}

func (a *CortexAdapter) StartAsync(ctx context.Context) error {
	go func() {
		a.transitionState(services.Starting, nil)

		if err := a.CloudTSModule.start(ctx); err != nil {
			a.transitionState(services.Failed, err) // 直接传递错误
			return
		}

		a.transitionState(services.Running, nil)
	}()
	return nil
}

func (a *CortexAdapter) AwaitRunning(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if a.State() == services.Running {
				return nil
			}
			if a.State() == services.Failed {
				return errors.New("service failed to start")
			}
		}
	}
}

func (a *CortexAdapter) StopAsync() {
	go func() {
		a.transitionState(services.Stopping, nil)

		if err := a.CloudTSModule.stop(nil); err != nil {
			a.transitionState(services.Failed, err) // 直接传递错误
			return
		}

		a.transitionState(services.Terminated, nil)
	}()
}

func (a *CortexAdapter) AwaitTerminated(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if a.State() == services.Terminated {
				return nil
			}
		}
	}
}

func (a *CortexAdapter) FailureCase() error {
	if a.State() == services.Failed {
		return errors.New("module in failed state")
	}
	return nil
}

func (a *CortexAdapter) State() services.State {
	a.mtx.Lock()
	defer a.mtx.Unlock()
	return a.state
}

func (a *CortexAdapter) AddListener(listener services.Listener) {
	a.mtx.Lock()
	defer a.mtx.Unlock()
	a.listeners = append(a.listeners, listener)
}
