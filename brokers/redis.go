package brokers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"gitlab.kube.2xi.com/yunwei/platform-backend-tasks/celery/message"
)

type RedisBroker struct {
	*redis.Pool
	Queues       []string
	taskProtocol int
}

func NewRedisBroker(url string, queues []string, taskProtocol int) (*RedisBroker, error) {
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
	return &RedisBroker{
		Pool:         redisPool,
		Queues:       queues,
		taskProtocol: taskProtocol,
	}, nil
}

func (b *RedisBroker) GetMessage() (*message.TaskMessage, error) { // must be non-blocking
	conn := b.Get()
	defer conn.Close()
	var args []interface{}
	for _, n := range b.Queues {
		args = append(args, n)
	}
	args = append(args, "1")
	messageJSON, err := conn.Do("BRPOP", args...)
	if err != nil {
		return nil, err
	}
	if messageJSON == nil {
		return nil, fmt.Errorf("null message received from redis")
	}
	messageList := messageJSON.([]interface{})
	//if string(messageList[0].([]byte)) != b.QueueName {
	//	return nil, fmt.Errorf("not a celery message: %v", messageList[0])
	//}
	msg := &message.CeleryMessage{}
	if err := json.Unmarshal(messageList[1].([]byte), &msg); err != nil {
		return nil, err
	}
	body, err := base64.StdEncoding.DecodeString(msg.Body)
	if err != nil {
		return nil, err
	}
	msg.Body = string(body)
	return msg.GetTaskMessage(), nil
}
func (b *RedisBroker) SendCeleryMessage(msg *message.CeleryMessage) error {
	msg.Body = base64.StdEncoding.EncodeToString([]byte(msg.Body))
	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	conn := b.Get()
	defer conn.Close()
	_, err = conn.Do("LPUSH", msg.Queue, jsonBytes)
	if err != nil {
		return err
	}
	return nil
}
