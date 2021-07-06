package celery_test

import (
	"fmt"
	"github.com/goodking-bq/go-celery"
	"time"
)

// quick start client
func Example_client() {
	// create some default config for celery app
	cfg := celery.DefaultConfig()
	// create new celery app with config
	app, err := celery.NewCelery(cfg)
	if err != nil {
		panic(err)
	}
	// call task with task name and args
	// return *message.AsyncResult witch you can use to get result
	delay, err := app.Delay("worker.add", celery.Args(1, 2))
	if err != nil {
		return
	}
	res, err := delay.Get(time.Second)
	if err != nil {
		return
	}
	fmt.Println(res)
	// Output:
	// 3
}

// quick start server
func Example_server() {
	// create some default config for celery app
	cfg := celery.DefaultConfig()
	// create new celery app with config
	app, err := celery.NewCelery(cfg)
	if err != nil {
		panic(err)
	}
	// create a new *Task
	// with worker.add is task name,call task use it
	// func do something
	add := celery.NewTask("worker.add", func(a, b int) int {
		return a + b
	})
	// register task
	app.Register(add)
	// run server
	app.StartWorker()
	// Output:
}

// ExampleArgs
func ExampleArgs() {
	app, _ := celery.NewCelery(celery.DefaultConfig())
	app.Delay("worker.add",
		celery.Args(1, 2), // delay with args
	)
}

// ExampleKwargs
func ExampleKwargs() {
	app, _ := celery.NewCelery(celery.DefaultConfig())
	app.Register(celery.NewTask("worker.add",
		func(a, b int) int { return a + b }).
		WithKwargs("a", "b"), // WithKwargs is necessary
	)
	app.Delay("worker.add",
		celery.Kwargs("a", 1, "b", 2), // delay with kwargs
	)
}
