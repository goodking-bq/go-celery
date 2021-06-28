package queue

import "github.com/streadway/amqp"

type AMQPExchangeType string

var (
	ExchangeDirect  AMQPExchangeType = "direct"
	ExchangeTopic   AMQPExchangeType = "topic"
	ExchangeFanout  AMQPExchangeType = "fanout"
	ExchangeHeaders AMQPExchangeType = "header"
)

type Exchange struct {
	Name       string           `json:"name"`
	Type       AMQPExchangeType `json:"type"`
	Durable    bool             `json:"durable"`
	AutoDelete bool             `json:"auto_delete"`
	Passive    bool             `json:"passive"`
	Internal   bool             `json:"internal"`
	NoWait     bool             `json:"no_wait"`
	Arguments  amqp.Table       `json:"arguments"`
}

func NewExchange(name string, t AMQPExchangeType, durable, authDelete bool) Exchange {
	return Exchange{
		Name:       name,
		Type:       t,
		Durable:    durable,
		AutoDelete: authDelete,
	}
}

type Queue struct {
	Name       string   `json:"name"`
	Exchange   Exchange `json:"exchange"`
	RoutingKey string   `json:"routing_key"`
}

type Queues []Queue

func (qs *Queues) Queue(routingKey string) Queue {
	for _, q := range *qs {
		if routingKey == q.RoutingKey {
			return q
		}
	}
	return Queue{"celery", Exchange{Name: "celery"}, "celery"}
}
