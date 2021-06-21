package celery

import (
	"context"
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
	return &Task{
		Name:   name,
		Func:   f,
		Ctx:    false,
		Kwargs: []string{},
	}
}
