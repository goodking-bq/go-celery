package brokers

import (
	"errors"
	"github.com/goodking-bq/go-celery/message"
	"github.com/streadway/amqp"
	"log"
	"strings"
)

type Broker interface {
	SendCeleryMessage(celeryMessage *message.CeleryMessage) error
	GetMessage() (*message.TaskMessage, error) // must be non-blocking
}

func NewBroker(url string, queues []string) (Broker, error) {
	if strings.HasPrefix(url, "redis") {
		return NewRedisBroker(url, queues, 2)
	} else if strings.HasPrefix(url, "amqp") {
		return NewAMQPBroker(url)
	} else {
		return nil, errors.New("not supported broker type")
	}
}

// deliveryAck acknowledges delivery message with retries on error
func deliveryAck(delivery amqp.Delivery) {
	var err error
	for retryCount := 3; retryCount > 0; retryCount-- {
		if err = delivery.Ack(false); err == nil {
			break
		}
	}
	if err != nil {
		log.Printf("amqp_backend: failed to acknowledge result message %+v: %+v", delivery.MessageId, err)
	}
}
