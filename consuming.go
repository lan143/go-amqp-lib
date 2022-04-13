package go_amqp_lib

import (
	"context"
	"github.com/streadway/amqp"
	"log"
	"sync"
)

type Consuming struct {
	shutdownChan chan<- interface{}

	consumers []*Consumer
	amqp      *Client
	wg        *sync.WaitGroup

	consumingMaxJobsCount uint64
	jobsCount             uint64
	jobsCountLock         sync.RWMutex
}

func (consuming *Consuming) Init(consumingMaxJobsCount uint64, wg *sync.WaitGroup, shutdownChan chan<- interface{}) {
	consuming.consumingMaxJobsCount = consumingMaxJobsCount
	consuming.wg = wg
	consuming.shutdownChan = shutdownChan
	consuming.jobsCount = 0
}

func (consuming *Consuming) AddConsumer(consumer Consumer) {
	consuming.consumers = append(consuming.consumers, &consumer)
}

func (consuming *Consuming) Run(ctx context.Context) {
	consuming.runConsumers(ctx)
}

func (consuming *Consuming) runConsumers(ctx context.Context) {
	var wgConsumers sync.WaitGroup
	consuming.wg.Add(1)

	go consuming.waitShutdown(ctx, consuming.wg, &wgConsumers, consuming.shutdownChan)

	log.Printf("amqp.consuming: found %d consumers", len(consuming.consumers))

	for _, c := range consuming.consumers {
		go consuming.runConsumer(ctx, c, &wgConsumers)
	}
}

func (consuming *Consuming) runConsumer(ctx context.Context, consumer *Consumer, wgConsumers *sync.WaitGroup) {
	log.Printf("amqp.consuming.(%s): start", (*consumer).GetQueueName())

	consume, err := consuming.amqp.Consume((*consumer).GetQueueName(), false, (*consumer).IsQuorum())
	if err != nil {
		panic(err)
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("amqp.consuming.(%s): shutdown", (*consumer).GetQueueName())
			return
		case delivery := <-consume.Delivery:
			if len(delivery.Body) == 0 {
				break
			}

			consuming.waitForHandleJob()
			go consuming.handleMessage(ctx, &delivery, consume, consumer, wgConsumers)
			break
		}
	}
}

func (consuming *Consuming) handleMessage(
	ctx context.Context,
	delivery *amqp.Delivery,
	consume *Consume,
	consumer *Consumer,
	wgConsumers *sync.WaitGroup,
) {
	wgConsumers.Add(1)
	defer wgConsumers.Done()
	(*consumer).Handle(ctx, consume.Channel, delivery, consuming.amqp)
	consuming.freeHandleJob()
}

func (consuming *Consuming) waitShutdown(
	ctx context.Context,
	wg *sync.WaitGroup,
	wgConsumers *sync.WaitGroup,
	shutdownChan chan<- interface{},
) {
	<-ctx.Done()
	log.Println("amqp.consuming: waiting for tasks to complete")
	wgConsumers.Wait()
	log.Println("amqp.consuming: tasks completed")
	wg.Done()
	consuming.amqp.Shutdown(ctx)
	close(shutdownChan)
}

func (consuming *Consuming) waitForHandleJob() {
	for {
		consuming.jobsCountLock.RLock()

		if consuming.jobsCount <= consuming.consumingMaxJobsCount {
			consuming.jobsCountLock.RUnlock()
			break
		} else {
			consuming.jobsCountLock.RUnlock()
		}
	}

	consuming.jobsCountLock.Lock()
	consuming.jobsCount++
	consuming.jobsCountLock.Unlock()
}

func (consuming *Consuming) freeHandleJob() {
	consuming.jobsCountLock.Lock()
	consuming.jobsCount--
	consuming.jobsCountLock.Unlock()
}

func NewConsuming(amqp *Client) *Consuming {
	return &Consuming{
		amqp: amqp,
	}
}
