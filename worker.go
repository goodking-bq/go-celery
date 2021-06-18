package celery

import (
	"context"
	"fmt"
	"github.com/goodking-bq/go-celery/message"
	"log"
	"os"
	"reflect"
	"runtime"
	"sync"
	"time"
)

type Worker struct {
	App             *Celery
	cancel          context.CancelFunc
	workWG          sync.WaitGroup
	rateLimitPeriod time.Duration
	Concurrency     int `json:"concurrency"`
	tasks           sync.Map
}

func (w *Worker) Register(name string, task interface{}) {
	w.tasks.Store(name, task)
}

func (w *Worker) PrintInfo() {
	name, _ := os.Hostname()
	fmt.Println("---------------celery@", name)
	fmt.Println("--- ***** -----")
	fmt.Println("--- ***** ----- power of ", runtime.Version())
	fmt.Println("- *** --- * --- ")
	fmt.Println("- ** ---------- [config]")
	fmt.Println("- ** ---------- .> app:", "gocelery")
	fmt.Println("- ** ---------- .> broker: ", w.App.Config.BrokerUrl)
	fmt.Println("- ** ---------- .> results: ", w.App.Config.BackendUrl)
	fmt.Println("- *** --- * --- .> concurrency: ", w.App.Config.Concurrency)
	fmt.Println("-- ******* ---- .>")
	fmt.Println("--- ***** ----- ")
	fmt.Println("-------------- [queues]")
	for _, q := range w.App.Config.Queues {
		fmt.Println("                .> ", q)
	}
	fmt.Println("")
	fmt.Println("[tasks]: ")
	w.tasks.Range(func(key, value interface{}) bool {
		fmt.Println("  - ", key)
		return true
	})
}

// StartWorker starts celery workers
func (w *Worker) StartWorker() {
	w.PrintInfo()
	w.StartWorkerWithContext(context.Background())
}

// StartWorkerWithContext starts celery worker(s) with given parent context
func (w *Worker) StartWorkerWithContext(ctx context.Context) {
	var wctx context.Context
	wctx, w.cancel = context.WithCancel(ctx)
	w.workWG.Add(w.Concurrency)
	for i := 0; i < w.Concurrency; i++ {
		go func(workerID int) {
			defer w.workWG.Done()
			ticker := time.NewTicker(w.rateLimitPeriod)
			for {
				select {
				case <-wctx.Done():
					return
				case <-ticker.C:
					// process task request
					celeryMessage, err := w.App.Broker.GetMessage() // get celery message

					if err != nil || celeryMessage == nil {
						continue
					}

					// run task
					resultMsg, err := w.RunTask(celeryMessage)
					if err != nil {
						log.Printf("failed to run task message %s: %+v", celeryMessage.ID, err)
						continue
					}
					// push result to backend
					err = w.App.Backend.SetResult(celeryMessage.ID, resultMsg)
					message.ReleaseResultMessage(resultMsg)
					if err != nil {
						log.Printf("failed to push result: %+v", err)
						continue
					}
				}
			}
		}(i)
	}
}

// StartWorkerForever starts celery workers forever
func (w *Worker) StartWorkerForever() {
	w.StartWorker()
	forever := make(chan bool)
	<-forever
}

// RunTask runs celery task
func (w *Worker) RunTask(msg *message.TaskMessage) (*message.CeleryResultMessage, error) {

	// ignore if the message is expired
	if msg.Expires != nil && msg.Expires.UTC().Before(time.Now().UTC()) {
		return nil, fmt.Errorf("task %s is expired on %s", msg.ID, msg.Expires)
	}

	// check for malformed task message - args cannot be nil
	if msg.Args == nil {
		return nil, fmt.Errorf("task %s is malformed - args cannot be nil", msg.Args)
	}

	// get task
	task := w.GetTask(msg.Task)
	if task == nil {
		return nil, fmt.Errorf("task %s is not registered", msg.Task)
	}

	// convert to task interface
	taskInterface, ok := task.(CeleryTask)
	if ok {
		if err := taskInterface.ParseKwargs(msg.Kwargs); err != nil {
			return nil, err
		}
		val, err := taskInterface.RunTask()
		if err != nil {
			return nil, err
		}
		return message.GetResultMessage(val), err
	}

	// use reflection to execute function ptr
	taskFunc := reflect.ValueOf(task)
	return runTaskFunc(&taskFunc, msg)
}

// GetTask retrieves registered task
func (w *Worker) GetTask(name string) interface{} {
	t, ok := w.tasks.Load(name)
	if !ok {
		return nil
	}
	return t
}

func runTaskFunc(taskFunc *reflect.Value, msg *message.TaskMessage) (*message.CeleryResultMessage, error) {

	// check number of arguments
	numArgs := taskFunc.Type().NumIn()
	messageNumArgs := len(msg.Args)
	if numArgs != messageNumArgs {
		return nil, fmt.Errorf("Number of task arguments %d does not match number of message arguments %d", numArgs, messageNumArgs)
	}

	// construct arguments
	in := make([]reflect.Value, messageNumArgs)
	for i, arg := range msg.Args {
		origType := taskFunc.Type().In(i).Kind()
		msgType := reflect.TypeOf(arg).Kind()
		// special case - convert float64 to int if applicable
		// this is due to json limitation where all numbers are converted to float64
		if origType == reflect.Int && msgType == reflect.Float64 {
			arg = int(arg.(float64))
		}
		if origType == reflect.Float32 && msgType == reflect.Float64 {
			arg = float32(arg.(float64))
		}

		in[i] = reflect.ValueOf(arg)
	}

	// call method
	res := taskFunc.Call(in)
	if len(res) == 0 {
		return nil, nil
	}

	return message.GetReflectionResultMessage(&res[0]), nil
}
