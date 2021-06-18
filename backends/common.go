package backends

import (
	"errors"
	"gitlab.kube.2xi.com/yunwei/platform-backend-tasks/celery/message"
	"strings"
)

// Backend is interface for celery backend database
type Backend interface {
	GetResult(string) (*message.CeleryResultMessage, error) // must be non-blocking
	SetResult(taskID string, result *message.CeleryResultMessage) error
}

func NewBackend(url string, taskProtocol int) (Backend, error) {
	if strings.HasPrefix(url, "redis") {
		return NewRedisBackend(url, taskProtocol)
	} else if strings.HasPrefix(url, "ampq") {
		return nil, nil
	} else {
		return nil, errors.New("not supported backend type")
	}
}
