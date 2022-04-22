package go_amqp_lib

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"log"
	"sync"
	"time"
)

type Client struct {
	connection    *amqp.Connection
	systemChannel *amqp.Channel
	config        AmqpConfig

	wg *sync.WaitGroup

	isConnected   bool
	isConnectedMu sync.RWMutex

	consumes []*Consume
}

type Consume struct {
	Name, QueueName                string
	Channel                        *amqp.Channel
	QueueTable                     amqp.Table
	Durable, Exclusive, AutoDelete bool
	Delivery                       <-chan amqp.Delivery
}

func (client *Client) Init(config AmqpConfig, wg *sync.WaitGroup) {
	log.Printf("amqp: init")

	client.config = config
	client.wg = wg
}

func (client *Client) Run(ctx context.Context) error {
	log.Printf("amqp: run")

	err := client.connect()
	if err != nil {
		return err
	}

	client.systemChannel, err = client.connection.Channel()
	if err != nil {
		return err
	}

	client.setConnected(true)
	client.wg.Add(1)

	go client.handleReconnect()

	return nil
}

func (client *Client) CloseConsume(consume *Consume) error {
	log.Printf("amqp.consume.close: %s", consume.QueueName)

	err := consume.Channel.Cancel(consume.Name, false)
	if err != nil {
		return err
	}

	err = consume.Channel.Close()

	for index, c := range client.consumes {
		if c.Name == consume.Name {
			client.consumes = append(client.consumes[:index], client.consumes[index+1:]...)
			break
		}
	}

	return err
}

func (client *Client) Shutdown(ctx context.Context) {
	var consumes []*Consume
	copy(consumes, client.consumes)

	for _, consume := range consumes {
		err := client.CloseConsume(consume)
		if err != nil {
			log.Printf("amqp.shutdown.consume.(%s): %s", consume.QueueName, err.Error())
		}
	}

	err := client.systemChannel.Close()
	if err != nil {
		log.Printf("amqp.shutdown.systemChannel: %s", err.Error())
	}

	err = client.connection.Close()
	if err != nil {
		log.Printf("amqp.shutdown.connection: %s", err.Error())
	}

	client.wg.Done()
}

func (client *Client) Consume(queueName string, isReplyBy bool, isQuorum bool) (*Consume, error) {
	client.waitConnection()

	var err error
	var durable, exclusive, autoDelete bool
	if isReplyBy {
		durable = false
		exclusive = true
		autoDelete = true
	} else {
		durable = true
		exclusive = false
		autoDelete = false
	}

	table := amqp.Table{}
	if isQuorum {
		table["x-queue-type"] = "quorum"
	} else {
		table["x-queue-mode"] = "default"
	}

	consume := Consume{
		Name:       uuid.New().String(),
		QueueName:  queueName,
		QueueTable: table,
		Durable:    durable,
		Exclusive:  exclusive,
		AutoDelete: autoDelete,
	}

	err = client.consumeInternal(&consume)

	if err != nil {
		return nil, err
	}

	log.Printf("amqp.consume.(%s): successful", consume.QueueName)
	client.consumes = append(client.consumes, &consume)

	return &consume, err
}

func (client *Client) Publish(
	channel *amqp.Channel,
	route string,
	body []byte,
	replyTo *string,
) error {
	client.waitConnection()

	headers := amqp.Table{}
	if replyTo != nil {
		headers["reply-to"] = *replyTo
	}

	if channel == nil {
		channel = client.systemChannel
	}

	err := channel.Publish(
		"",
		route,
		false,
		false,
		amqp.Publishing{
			MessageId:    uuid.New().String(),
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         body,
			Headers:      headers,
		})
	if err != nil {
		return err
	}

	log.Printf("amqp.publish.(%s): %s", route, body)

	return nil
}

func (client *Client) IsReady() bool {
	return client.isConnected
}

func (client *Client) connect() error {
	var c string
	if client.config.Username == "" {
		c = fmt.Sprintf(
			"amqp://%s:%s%s",
			client.config.Host,
			client.config.Port,
			client.config.VHost,
		)
	} else {
		c = fmt.Sprintf(
			"amqp://%s:%s@%s:%s%s",
			client.config.Username,
			client.config.Password,
			client.config.Host,
			client.config.Port,
			client.config.VHost,
		)
	}

	var err error
	client.connection, err = amqp.Dial(c)

	return err
}

func (client *Client) setConnected(connected bool) {
	client.isConnectedMu.Lock()
	client.isConnected = connected
	client.isConnectedMu.Unlock()
}

func (client *Client) waitConnection() {
	for {
		client.isConnectedMu.RLock()
		if client.isConnected {
			client.isConnectedMu.RUnlock()
			return
		} else {
			client.isConnectedMu.RUnlock()
		}
	}
}

func (client *Client) handleReconnect() {
	connClose := client.connection.NotifyClose(make(chan *amqp.Error))
	chClose := client.systemChannel.NotifyClose(make(chan *amqp.Error))

	for {
		select {
		case err := <-connClose:
			if err != nil {
				log.Printf("amqp.connection.close: %s", err.Error())
				client.setConnected(false)

				for {
					err := client.connect()
					if err != nil {
						log.Printf("amqp.connection.reconnect: %s", err.Error())
						time.Sleep(1 * time.Second)
					} else {
						log.Printf("amqp.connection.reconnect: successful")
						connClose = client.connection.NotifyClose(make(chan *amqp.Error))
						break
					}
				}
			}
			break
		case err := <-chClose:
			if err != nil {
				log.Printf("amqp.channel.close: %s", err.Error())
				client.setConnected(false)

				for {
					var err error
					client.systemChannel, err = client.connection.Channel()

					if err != nil {
						log.Printf("amqp.channel.error: %s", err.Error())
						time.Sleep(1 * time.Second)
					} else {
						log.Printf("amqp.system-channel.restore: successful")

						chClose = client.systemChannel.NotifyClose(make(chan *amqp.Error))
						client.setConnected(true)
						break
					}
				}
			}
			break
		}
	}
}

func (client *Client) handleConsumeChannelClose(consume *Consume) {
	chClose := consume.Channel.NotifyClose(make(chan *amqp.Error))
	err := <-chClose
	if err != nil {
		log.Printf("amqp.consume.(%s).channel.close: %s", consume.QueueName, err.Error())
		for {
			err := client.consumeInternal(consume)
			if err != nil {
				log.Printf("amqp.consume.(%s).restore: %s", consume.QueueName, err.Error())
				time.Sleep(1 * time.Second)
			} else {
				log.Printf("amqp.consume.(%s).restore: successful", consume.QueueName)
				break
			}
		}
	}
}

func (client *Client) consumeInternal(consume *Consume) error {
	var err error
	consume.Channel, err = client.connection.Channel()
	if err != nil {
		return nil
	}

	go client.handleConsumeChannelClose(consume)

	queue, err := consume.Channel.QueueDeclare(
		consume.QueueName,
		consume.Durable,
		consume.AutoDelete,
		consume.Exclusive,
		false,
		consume.QueueTable,
	)
	if err != nil {
		return err
	}

	if len(consume.QueueName) == 0 {
		consume.QueueName = queue.Name
	}

	consume.Delivery, err = consume.Channel.Consume(
		consume.QueueName,
		consume.Name,
		false,
		false,
		false,
		false,
		nil,
	)

	return err
}

func NewClient() *Client {
	return &Client{}
}
