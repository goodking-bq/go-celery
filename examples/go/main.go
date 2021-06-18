package main

import "github.com/goodking-bq/go-celery"

type MyConfig struct {
	celery.Celery
	My string
}
