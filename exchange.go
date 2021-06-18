package celery

type AMQPExchangeType string

var (
	DirectType AMQPExchangeType = "direct"
	TopicType  AMQPExchangeType = "topic"
	FanoutType AMQPExchangeType = "fanout"
	HeaderType AMQPExchangeType = "header"
)

type exchange struct {
	Name string           `json:"name"`
	Type AMQPExchangeType `json:"type"`
}

type Queue struct {
	Name       string   `json:"name"`
	Exchange   exchange `json:"exchange"`
	RoutingKey string   `json:"routing_key"`
}

type Queues []Queue

func (qs *Queues) Queue(routing_key string) Queue {
	return Queue{}
}

type DirectExchange struct {
	RoutingKey string
}

type TopicExchange struct {
}

type FanoutExchange struct {
}
