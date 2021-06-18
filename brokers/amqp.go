// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package brokers

import (
	"encoding/json"
	"fmt"
	"gitlab.kube.2xi.com/yunwei/platform-backend-tasks/celery/message"
	"time"

	"github.com/streadway/amqp"
)

// AMQPExchange stores AMQP Exchange configuration
type AMQPExchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
}

// NewAMQPExchange creates new AMQPExchange
func NewAMQPExchange(name string) *AMQPExchange {
	return &AMQPExchange{
		Name:       name,
		Type:       "direct",
		Durable:    true,
		AutoDelete: true,
	}
}

// AMQPQueue stores AMQP Queue configuration
type AMQPQueue struct {
	Name       string
	Durable    bool
	AutoDelete bool
}

// NewAMQPQueue creates new AMQPQueue
func NewAMQPQueue(name string) *AMQPQueue {
	return &AMQPQueue{
		Name:       name,
		Durable:    true,
		AutoDelete: false,
	}
}

//AMQPBroker is RedisBroker for AMQP
type AMQPBroker struct {
	*amqp.Channel
	Connection       *amqp.Connection
	Exchange         *AMQPExchange
	Queue            *AMQPQueue
	consumingChannel <-chan amqp.Delivery
	Rate             int
	taskProtocol     int
}

// NewAMQPConnection creates new AMQP channel
func NewAMQPConnection(host string) (*amqp.Connection, *amqp.Channel, error) {
	connection, err := amqp.Dial(host)
	if err != nil {
		return nil, nil, err
	}

	channel, err := connection.Channel()
	if err != nil {
		return nil, nil, err
	}
	return connection, channel, nil
}

// NewAMQPBroker creates new AMQPBroker
func NewAMQPBroker(host string) (*AMQPBroker, error) {
	conn, channel, err := NewAMQPConnection(host)
	if err != nil {
		return nil, err
	}
	return NewAMQPBrokerByConnAndChannel(conn, channel)
}

// NewAMQPBrokerByConnAndChannel creates new AMQPBroker using AMQP conn and channel
func NewAMQPBrokerByConnAndChannel(conn *amqp.Connection, channel *amqp.Channel) (*AMQPBroker, error) {
	broker := &AMQPBroker{
		Channel:      channel,
		Connection:   conn,
		Exchange:     NewAMQPExchange("default"),
		Queue:        NewAMQPQueue("celery"),
		Rate:         4,
		taskProtocol: 2,
	}
	if err := broker.CreateExchange(); err != nil {
		return nil, err
	}
	if err := broker.CreateQueue(); err != nil {
		return nil, err
	}
	if err := broker.Qos(broker.Rate, 0, false); err != nil {
		return nil, err
	}
	if err := broker.StartConsumingChannel(); err != nil {
		return nil, err
	}
	return broker, nil
}

// StartConsumingChannel spawns receiving channel on AMQP queue
func (b *AMQPBroker) StartConsumingChannel() error {
	channel, err := b.Consume(b.Queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	b.consumingChannel = channel
	return nil
}

// SendCeleryMessage sends CeleryMessage to broker
func (b *AMQPBroker) SendCeleryMessage(msg *message.CeleryMessage) error {
	queueName := "celery"
	_, err := b.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // autoDelete
		false,     // exclusive
		false,     // noWait
		nil,       // args
	)
	if err != nil {
		return err
	}
	err = b.ExchangeDeclare(
		"default",
		"direct",
		true,
		true,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	resBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	publishMessage := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		Body:         resBytes,
	}

	return b.Publish(
		"",
		queueName,
		false,
		false,
		publishMessage,
	)
}

// GetMessage retrieves task message from AMQP queue
func (b *AMQPBroker) GetMessage() (*message.TaskMessage, error) {
	select {
	case delivery := <-b.consumingChannel:
		deliveryAck(delivery)
		celeryMessage := message.GetCeleryMessage(string(delivery.Body))
		celeryMessage.Headers = delivery.Headers
		celeryMessage.ContentType = delivery.ContentType
		celeryMessage.ContentEncoding = delivery.ContentEncoding
		celeryMessage.Queue = delivery.RoutingKey
		celeryMessage.Properties = message.CeleryMessageProperties{
			BodyEncoding:  "base64",
			CorrelationID: delivery.CorrelationId,
			ReplyTo:       delivery.ReplyTo,
			DeliveryInfo: message.CeleryMessagePropertiesDeliveryInfo{
				RoutingKey: delivery.RoutingKey,
				Exchange:   delivery.Exchange,
			},
			DeliveryMode: 2,
			DeliveryTag:  "",
			Priority:     0,
		}
		taskMessage := celeryMessage.GetTaskMessage()
		defer message.ReleaseCeleryMessage(celeryMessage)
		return taskMessage, nil
	default:
		return nil, fmt.Errorf("consuming channel is empty")
	}

}

// CreateExchange declares AMQP exchange with stored configuration
func (b *AMQPBroker) CreateExchange() error {
	return b.ExchangeDeclare(
		b.Exchange.Name,
		b.Exchange.Type,
		b.Exchange.Durable,
		b.Exchange.AutoDelete,
		false,
		false,
		nil,
	)
}

// CreateQueue declares AMQP Queue with stored configuration
func (b *AMQPBroker) CreateQueue() error {
	_, err := b.QueueDeclare(
		b.Queue.Name,
		b.Queue.Durable,
		b.Queue.AutoDelete,
		false,
		false,
		nil,
	)
	return err
}
