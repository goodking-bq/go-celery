package celery

import "github.com/goodking-bq/go-celery/message"

type delayFunc func(t *message.TaskMessage, op *TaskOptions)

func (f delayFunc) apply(t *message.TaskMessage, op *TaskOptions) {
	f(t, op)
}

func Args(args ...interface{}) Delayer {
	return delayFunc(func(task *message.TaskMessage, option *TaskOptions) {
		task.Args = args
	})
}
func Kwargs(args ...interface{}) Delayer {
	return delayFunc(func(task *message.TaskMessage, option *TaskOptions) {
		kwargs := map[string]interface{}{}
		for i := 0; i < len(args); i += 2 {
			kwargs[args[i].(string)] = args[i+1]
		}
		task.Kwargs = kwargs
	})
}

func KwargsMap(kw map[string]interface{}) Delayer {
	return delayFunc(func(task *message.TaskMessage, option *TaskOptions) {
		task.Kwargs = kw
	})
}

func Countdown(v int) Delayer {
	return delayFunc(func(task *message.TaskMessage, option *TaskOptions) {
		option.Countdown = v
	})
}

func Retry(v bool) Delayer {
	return delayFunc(func(task *message.TaskMessage, option *TaskOptions) {
		option.Retry = v
	})
}
