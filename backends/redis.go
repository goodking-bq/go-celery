package backends

import (
	"encoding/json"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/goodking-bq/go-celery/message"
)

type RedisBackend struct {
	*redis.Pool
	taskProtocol int
}

func NewRedisBackend(url string, taskProtocol int) (*RedisBackend, error) {
	// create redis connection pool
	redisPool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL(url)
			if err != nil {
				return nil, err
			}
			return c, err
		},
	}
	return &RedisBackend{
		Pool:         redisPool,
		taskProtocol: taskProtocol,
	}, nil
}

// GetResult queries redis backend to get asynchronous result
func (cb *RedisBackend) GetResult(taskID string) (*message.CeleryResultMessage, error) {
	conn := cb.Get()
	defer conn.Close()
	val, err := conn.Do("GET", fmt.Sprintf("celery-task-meta-%s", taskID))
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, fmt.Errorf("result not available")
	}
	var resultMessage message.CeleryResultMessage
	err = json.Unmarshal(val.([]byte), &resultMessage)
	if err != nil {
		return nil, err
	}
	return &resultMessage, nil
}

// SetResult pushes result back into redis backend
func (cb *RedisBackend) SetResult(taskID string, result *message.CeleryResultMessage) error {
	defer message.ReleaseResultMessage(result)
	result.TaskId = taskID
	resBytes, err := json.Marshal(result)
	if err != nil {
		return err
	}
	conn := cb.Get()
	defer conn.Close()
	_, err = conn.Do("SETEX", fmt.Sprintf("celery-task-meta-%s", taskID), 86400, resBytes)
	return err
}
