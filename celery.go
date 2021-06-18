package celery

import (
	"context"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/goodking-bq/go-celery/backends"
	"github.com/goodking-bq/go-celery/brokers"
	"github.com/goodking-bq/go-celery/message"
	"github.com/spf13/viper"
	"sync"
	"time"
)

type Celery struct {
	Broker  brokers.Broker
	Backend backends.Backend
	Config  *Config
	worker  *Worker
	beat    *Beat
	// ctx celery app context，
	// default include
	ctx Context
}

// NewCelery create celery app
func NewCelery(config *Config) (*Celery, error) {
	broker, err := brokers.NewBroker(config.BrokerUrl, config.Queues)
	if err != nil {
		return nil, err
	}
	backend, err := backends.NewBackend(config.BackendUrl, 1)
	if err != nil {
		return nil, err
	}
	app := &Celery{Config: config, Broker: broker, Backend: backend}
	worker := &Worker{
		App:             app,
		rateLimitPeriod: 1 * time.Second,
		Concurrency:     config.Concurrency,
		tasks:           sync.Map{},
	}
	app.worker = worker
	app.ctx = Context{App: app}
	return app, nil
}

// NewCeleryWithConfigFile create celery app use config file
func NewCeleryWithConfigFile(file string, conf ...interface{}) (*Celery, error) {
	celeryConf := DefaultConfig()
	viper.SetConfigFile(file)
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
	var vUnmarshal = func(celeryConf *Config, conf ...interface{}) {
		// save config file value to celery config
		if err := viper.Unmarshal(celeryConf); err != nil {
			panic(fmt.Errorf("unmarshal conf failed, err:%s \n", err))
		}
		// save config file value to custom config
		for _, v := range conf {
			if err := viper.Unmarshal(v); err != nil {
				panic(fmt.Errorf("unmarshal conf failed, err:%s \n", err))
			}
		}
	}
	vUnmarshal(celeryConf, conf...)
	// watch config file
	viper.WatchConfig()
	viper.OnConfigChange(func(in fsnotify.Event) {
		fmt.Printf("config file <%s> is changed...", file)
		vUnmarshal(celeryConf, conf...)
	})
	return NewCelery(celeryConf)
}

func (c *Celery) Context() Context {
	return c.ctx
}

// Register register a task with name and func
func (c *Celery) Register(name string, task interface{}) {
	c.worker.Register(name, task)
}

func (c *Celery) Delay(name string, args ...interface{}) (*message.AsyncResult, error) {
	task := message.GetTaskMessage()
	task.Args = args
	task.Task = name
	return c.delay(task)
}

func (c *Celery) delay(task *message.TaskMessage) (*message.AsyncResult, error) {
	defer message.ReleaseTaskMessage(task)
	var encodeFun func(taskMessage *message.TaskMessage) (string, error)
	celeryMessage := message.GetCeleryMessage("")
	if c.Config.TaskProtocol == 1 {
		encodeFun = message.EncodeMessageBodyV1
	} else {
		encodeFun = message.EncodeMessageBodyV2
		celeryMessage.Headers = task.ToHeader()
	}
	body, err := encodeFun(task)
	if err != nil {
		return nil, err
	}
	celeryMessage.Body = body
	celeryMessage.Queue = "celery"
	err = c.Broker.SendCeleryMessage(celeryMessage)
	if err != nil {
		return nil, err
	}
	return &message.AsyncResult{
		TaskID:  task.ID,
		Backend: c.Backend,
	}, nil
}

// StartWorkerWithContext starts celery workers with given parent context
func (c *Celery) StartWorkerWithContext(ctx context.Context) {
	c.worker.StartWorkerWithContext(ctx)
}

// StartWorker starts celery workers
func (c *Celery) StartWorker() {
	c.worker.StartWorker()
}

// StartWorkerForever starts celery workers
func (c *Celery) StartWorkerForever() {
	c.worker.StartWorkerForever()
}

func (c *Celery) StartBeat() {
	c.beat.Start()
}

// CeleryTask is an interface that represents actual task
// Passing CeleryTask interface instead of function pointer
// avoids reflection and may have performance gain.
// ResultMessage must be obtained using GetResultMessage()
type CeleryTask interface {
	// ParseKwargs - define a method to parse kwargs
	ParseKwargs(map[string]interface{}) error

	// RunTask - define a method for execution
	RunTask() (interface{}, error)
}

func (c *Celery) RegisterTask(task *celeryTask) {
	c.worker.Register(task.Name, task)
}

type Context struct {
	context.Context
	App *Celery
}

// SetStatus can set custom status when a task call in
func (ctx Context) SetStatus(status string) {

}
