package celery

import (
	"context"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/goodking-bq/go-celery/backends"
	"github.com/goodking-bq/go-celery/brokers"
	"github.com/goodking-bq/go-celery/message"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

type Celery struct {
	name    string
	Broker  brokers.Broker
	Backend backends.Backend
	Log     *zap.SugaredLogger
	Config  *Config
	worker  *Worker
	beat    *Beat
	// ctx celery app context，
	// default include
	ctx Context
}

// NewCelery create celery app
func NewCelery(config *Config) (*Celery, error) {
	logger := NewLog(config.Name, config.Log)
	broker, err := brokers.NewBroker(config.BrokerUrl, config.Queues)
	if err != nil {
		return nil, err
	}
	backend, err := backends.NewBackend(config.BackendUrl, 1)
	if err != nil {
		return nil, err
	}
	app := &Celery{Config: config, Broker: broker,
		Backend: backend,
		Log:     logger,
	}
	worker := NewWorker(app)

	app.worker = worker
	app.ctx = Context{App: app}
	return app, nil
}

// NewCeleryWithConfigFile create celery app use config file
func NewCeleryWithConfigFile(file string, custom ...interface{}) (*Celery, error) {
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
	vUnmarshal(celeryConf, custom...)
	// watch config file
	viper.WatchConfig()
	viper.OnConfigChange(func(in fsnotify.Event) {
		fmt.Printf("config file <%s> is changed...", file)
		vUnmarshal(celeryConf, custom...)
	})
	return NewCelery(celeryConf)
}

func (c *Celery) Context() Context {
	return c.ctx
}

// RegisterFunc register a task with name and func
func (c *Celery) RegisterFunc(name string, f interface{}) {
	task := NewTask(name, f)
	c.worker.Register(task)
}

type Delayer interface {
	apply(t *message.TaskMessage, op *TaskOptions)
}

// Delay call task
func (c *Celery) Delay(name string, fs ...Delayer) (*message.AsyncResult, error) {
	task := message.GetTaskMessage()
	task.Task = name
	options := EmptyTaskOptions()
	for _, f := range fs {
		f.apply(task, options)
	}
	return c.delay(task, EmptyTaskOptions())
}

func (c *Celery) delay(task *message.TaskMessage, options *TaskOptions) (*message.AsyncResult, error) {
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

// StartWorkerForever starts celery workers forever
func (c *Celery) StartWorkerForever() {
	c.worker.StartWorker()
	forever := make(chan bool)
	<-forever
}

func (c *Celery) StartBeat() {
	c.beat.Start()
}

func (c *Celery) Register(tasks ...*Task) {
	for _, task := range tasks {
		if task.Ctx == true {
			task.Context.App = c
		}
		c.worker.Register(task)
	}
}

func (c *Celery) SetQueues(queue amqp.Queue) {

}

type Context struct {
	context.Context
	Msg *message.TaskMessage
	App *Celery
}

// SetStatus can set custom status when a task call in
func (ctx Context) SetStatus(status string) {
	resMsg := message.GetResultMessage(nil)
	resMsg.TaskId = ctx.Msg.ID
	resMsg.Status = status
	err := ctx.App.Backend.SetResult(ctx.Msg.ID, resMsg)
	if err != nil {
		ctx.App.Log.With(zap.String("task_id", resMsg.TaskId)).Errorf("set task status error: %s", err.Error())
	}
}

// Retry retry task
func (ctx Context) Retry() {

}
