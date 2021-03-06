package celery

import (
	"context"
	"testing"
	"time"
)

func TestContext(t *testing.T) {
	ctx := Context{}
	ctx.Context = context.WithValue(ctx, "zabbix", "a")
	t.Log(ctx.Value("zabbix").(string))
}

func newApp() *Celery {
	type MyConfig struct {
		My string `json:"my"`
	}
	conf := &MyConfig{}
	app, err := NewCeleryWithConfigFile("./config_test.yaml", conf)
	if err != nil {
		panic(err)
	}
	task := NewTask("worker.add", func(a, b int) int {
		_ = a / b //test panic
		return a + b
	}).WithKwargs("a", "b")
	taskCtx := NewTask("worker.add_ctx", func(ctx Context, a, b int) int {
		println(ctx.Value("custom").(string))
		return a + b
	}).WithValue("custom", "this is ctx").WithCtx().WithKwargs("a", "b")
	app.Register(task, taskCtx)
	return app
}
func TestNewCeleryWithConfigFile(t *testing.T) {
	app := newApp()
	app.StartWorker()
}

func TestCelery_Delay(t *testing.T) {
	app := newApp()
	task, err := app.Delay("worker.add",
		Args(1),
		//Kwargs("a", 1, "b", 2),
		KwargsMap(map[string]interface{}{"a": 1, "b": 2}),
	)

	if err != nil {
		t.Error(err)
	}
	if err := task.Wait(1 * time.Second); err != nil {
		t.Error(err)
	}
	if task.Successful() == false {
		t.Error(task.Get(1 * time.Second))
	} else {

		t.Logf("worker.add_ctx(1,2)=%f", task.Result())
	}
}
