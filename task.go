package celery

import (
	"context"
	"reflect"
)

type Kwarg struct {
	Name string
	T    reflect.Kind
}

type Kwargs struct {
	Values map[string]interface{}
	Types  []Kwarg
}

func (kw *Kwargs) Get(name string) {

}

type celeryTask struct {
	Name    string
	Kwargs  Kwargs
	Func    interface{}
	Ctx     bool
	Context Context
}

// WithCtx set call func with celery.Context as first params like add(ctx celery.Context,a,b int)
func (t *celeryTask) WithCtx() *celeryTask {
	t.Ctx = true
	return t
}

func (t *celeryTask) WithValue(name string, value interface{}) *celeryTask {
	t.Context.Context = context.WithValue(t.Context, name, value)
	return t
}

func (t *celeryTask) WithKW(name string, kwType reflect.Kind) *celeryTask {
	kw := Kwarg{
		Name: name,
		T:    kwType,
	}
	t.Kwargs.Types = append(t.Kwargs.Types, kw)
	return t
}

func Task(name string, f interface{}) *celeryTask {
	return &celeryTask{
		Name: name,
		Func: f,
		Ctx:  false,
		Kwargs: Kwargs{
			Values: map[string]interface{}{},
			Types:  []Kwarg{},
		},
	}
}
