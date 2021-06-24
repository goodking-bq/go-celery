package celery

import (
	"context"
	"sync"
	"time"
)

type Task struct {
	Name    string
	Kwargs  []string
	Func    interface{}
	Ctx     bool
	Context Context
}

// WithCtx set call func with celery.Context as first params like add(ctx celery.Context,a,b int)
// celery.Context context.Context
func (t *Task) WithCtx() *Task {
	t.Ctx = true
	return t
}

// WithValue when use WithCtx, you can use this,c
func (t *Task) WithValue(name string, value interface{}) *Task {
	t.Context.Context = context.WithValue(t.Context, name, value)
	return t
}

// WithKwargs set kwargs ,can call func like python kwargs
/*
example:
	add.apply_async(args=(1,), kwargs={"b": 2878}, serializer='json')
*/
func (t *Task) WithKwargs(kws ...string) *Task {
	t.Kwargs = kws
	return t
}

// NewTask new Task with name and func
func NewTask(name string, f interface{}) *Task {
	ctx := Context{App: nil}
	return &Task{
		Name:    name,
		Func:    f,
		Ctx:     false,
		Kwargs:  []string{},
		Context: ctx,
	}
}

// TaskOptions the task run options
type TaskOptions struct {
	Countdown    int           `json:"countdown"` // Number of seconds into the future that the task should execute.  Defaults to immediate execution.
	Retry        bool          `json:"retry"`     // task can retry? default is false
	MaxRetries   int           `json:"max_retries"`
	IgnoreResult bool          `json:"ignore_result"`
	TimeLimit    time.Duration `json:"time_limit"`
	Eta          time.Time     `json:"eta"`           //  Absolute time and date of when the task should be executed
	ExpireTime   time.Time     `json:"expire_time"`   // Datetime in the future for the task should expire
	ExpireSecond int           `json:"expire_second"` // seconds in the future for the task should expire
	Queue        string
}

var taskOptionsPool = sync.Pool{
	New: func() interface{} {
		return &TaskOptions{
			Countdown:    0,
			Retry:        false,
			MaxRetries:   0,
			IgnoreResult: false,
			TimeLimit:    0,
			Eta:          time.Time{},
			ExpireTime:   time.Time{},
			ExpireSecond: 0,
			Queue:        "",
		}
	},
}

func EmptyTaskOptions() *TaskOptions {
	return taskOptionsPool.Get().(*TaskOptions)
}
