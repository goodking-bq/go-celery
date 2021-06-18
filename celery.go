package celery

import (
	"context"
	"github.com/goodking-bq/go-celery/backends"
	"github.com/goodking-bq/go-celery/brokers"
	"github.com/goodking-bq/go-celery/message"
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
	ctx := Context{}
	ctx.Context = context.WithValue(ctx, "backend", backend)
	return app, nil
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
}

// SetStatus can set custom status when a task call in
func (ctx Context) SetStatus(status string) {

}
