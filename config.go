package celery

import (
	"runtime"
)

var (
	C *Config
)

func init() {
	C = &Config{
		BrokerUrl:    "redis://",
		BackendUrl:   "redis://",
		Timezone:     "Asia/Shanghai",
		Queues:       []string{"celery"},
		Concurrency:  runtime.NumCPU(),
		TaskProtocol: 2,
		EnableUTC:    true,
	}
}

type Config struct {
	BrokerUrl    string   `json:"broker_url"`
	BackendUrl   string   `json:"backend_url"`
	Timezone     string   `json:"timezone"`
	Queues       []string `json:"queue"` // can multi queue
	Concurrency  int      `json:"concurrency"`
	TaskProtocol int      `json:"task_protocol"`
	EnableUTC    bool     `json:"enable_utc"`
}

func DefaultConfig() *Config {
	return C
}
