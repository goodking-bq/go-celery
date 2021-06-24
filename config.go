package celery

import (
	"runtime"
)

var (
	C *Config
)

func init() {
	C = &Config{
		Name:         "celery",
		BrokerUrl:    "redis://",
		BackendUrl:   "redis://",
		Timezone:     "Asia/Shanghai",
		Queues:       []string{"celery"},
		Concurrency:  runtime.NumCPU(),
		TaskProtocol: 2,
		EnableUTC:    true,
		Log: logConfig{
			Level: "info",
			File:  "celery.log",
			Path:  "./log",
		},
	}
}

type logConfig struct {
	Level string `json:"level"`
	File  string `json:"log"`
	Path  string `json:"path"`
}

type Config struct {
	Name         string    `json:"name" yaml:"name"`
	BrokerUrl    string    `json:"broker_url" yaml:"broker_url"`
	BackendUrl   string    `json:"backend_url" yaml:"backend_url"`
	Timezone     string    `json:"timezone" yaml:"timezone"`
	Queues       []string  `json:"queues" yaml:"queues"` // can multi queue
	Concurrency  int       `json:"concurrency" yaml:"concurrency"`
	TaskProtocol int       `json:"task_protocol" yaml:"task_protocol"`
	EnableUTC    bool      `json:"enable_utc" yaml:"enable_utc"`
	Log          logConfig `json:"log" yaml:"log"`
}

func DefaultConfig() *Config {
	return C
}
