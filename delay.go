package celery

import (
	"github.com/goodking-bq/go-celery/message"
	"time"
)

type Delayer interface {
	apply(t *message.TaskMessage, op *TaskOptions)
}

type delayFunc func(t *message.TaskMessage, op *TaskOptions)

func (f delayFunc) apply(t *message.TaskMessage, op *TaskOptions) {
	f(t, op)
}

// Args func is set the args while celery delay task
func Args(args ...interface{}) Delayer {
	return delayFunc(func(task *message.TaskMessage, option *TaskOptions) {
		task.Args = args
	})
}

// Kwargs func is set the keywords params while celery delay task
//
// and the Task you must use WithKwargs to let me know function arg names and orders
func Kwargs(args ...interface{}) Delayer {
	return delayFunc(func(task *message.TaskMessage, option *TaskOptions) {
		kwargs := map[string]interface{}{}
		for i := 0; i < len(args); i += 2 {
			kwargs[args[i].(string)] = args[i+1]
		}
		task.Kwargs = kwargs
	})
}

// KwargsMap is like Kwargs but input  map[string]interface{}
func KwargsMap(kw map[string]interface{}) Delayer {
	return delayFunc(func(task *message.TaskMessage, option *TaskOptions) {
		task.Kwargs = kw
	})
}

// Countdown set  the task execute in many seconds
func Countdown(v time.Duration) Delayer {
	eta := time.Now().Add(time.Second * v).Format(time.RFC3339)
	return delayFunc(func(task *message.TaskMessage, option *TaskOptions) {
		task.ETA = &eta
	})
}

// Eta The ETA (estimated time of arrival) lets you set a specific date and time that is the earliest time at which your task will be executed.
func Eta(v *time.Time) Delayer {
	eta := v.Format("2006-01-02")
	return delayFunc(func(task *message.TaskMessage, option *TaskOptions) {
		task.ETA = &eta
	})
}

// Retry set times to the task can retry
func Retry(v bool) Delayer {
	return delayFunc(func(task *message.TaskMessage, option *TaskOptions) {
		option.Retry = v
	})
}

// IgnoreResult set disable result storage
func IgnoreResult() Delayer {
	return delayFunc(func(task *message.TaskMessage, option *TaskOptions) {
		option.IgnoreResult = true
	})
}

// Expires defines task execute expiry time
func Expires(t time.Time) Delayer {
	return delayFunc(func(task *message.TaskMessage, option *TaskOptions) {
		task.Expires = &message.CeleryTime{Time: t}
	})
}

// ExpireDuration defines task execute expiry time
func ExpireDuration(d time.Duration) Delayer {
	t := time.Now().Add(d)
	return delayFunc(func(task *message.TaskMessage, option *TaskOptions) {
		task.Expires = &message.CeleryTime{Time: t}
	})
}
