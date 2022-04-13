package amqp_lib

import (
	"context"
	"github.com/streadway/amqp"
)

type Consumer interface {
	GetQueueName() string
	IsQuorum() bool
	Handle(ctx context.Context, channel *amqp.Channel, delivery *amqp.Delivery, amqp *Client)
}
