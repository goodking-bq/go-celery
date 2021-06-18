package celery

import (
	"context"
	"reflect"
	"testing"
)

func TestContext(t *testing.T) {
	ctx := Context{}
	ctx.Context = context.WithValue(ctx, "zabbix", "a")
	t.Log(ctx.Value("zabbix").(string))
}

func TestNewCeleryWithConfigFile(t *testing.T) {
	type MyConfig struct {
		My string `json:"my"`
	}
	conf := &MyConfig{}
	app, err := NewCeleryWithConfigFile("./config_test.yaml", conf)
	if err != nil {
		println(err.Error())
	}
	task := Task("add", func(a, b int) int { return a + b }).
		WithCtx().WithKW("a", reflect.Int).WithKW("b", reflect.Int)
	app.RegisterTask(task)
}
