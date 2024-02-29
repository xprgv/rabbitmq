package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/xprgv/rabbitmq/pkg/logger"
)

type Channel struct {
	channel *amqp.Channel

	mtx            sync.RWMutex
	closed         *atomic.Bool
	logger         logger.Logger
	reconnectDelay time.Duration
}

func (ch *Channel) IsClosed() bool { return ch.closed.Load() }

func (ch *Channel) Close() error {
	if ch.IsClosed() {
		return amqp.ErrClosed
	}

	ch.closed.Store(true)

	ch.mtx.RLock()
	defer ch.mtx.RUnlock()

	return ch.channel.Close()
}

func (ch *Channel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	ch.mtx.RLock()
	defer ch.mtx.RUnlock()

	return ch.channel.NotifyClose(c)
}

func (ch *Channel) Get(queue string, autoAck bool) (msg amqp.Delivery, ok bool, err error) {
	ch.mtx.RLock()
	defer ch.mtx.RUnlock()

	return ch.channel.Get(queue, autoAck)
}

func (ch *Channel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	ch.mtx.RLock()
	defer ch.mtx.RUnlock()

	return ch.channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

func (ch *Channel) ExchangeDelete(name string, ifUnused, noWait bool) error {
	ch.mtx.RLock()
	defer ch.mtx.RUnlock()

	return ch.channel.ExchangeDelete(name, ifUnused, noWait)
}

func (ch *Channel) ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error {
	ch.mtx.RLock()
	defer ch.mtx.RUnlock()

	return ch.channel.ExchangeBind(destination, key, source, noWait, args)
}

func (ch *Channel) ExchangeUnbind(destination, key, source string, noWait bool, args amqp.Table) error {
	ch.mtx.RLock()
	defer ch.mtx.RUnlock()

	return ch.channel.ExchangeUnbind(destination, key, source, noWait, args)
}

func (ch *Channel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	ch.mtx.RLock()
	defer ch.mtx.RUnlock()

	return ch.channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

func (ch *Channel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	ch.mtx.RLock()
	defer ch.mtx.RUnlock()

	return ch.channel.QueueBind(name, key, exchange, noWait, args)
}

func (ch *Channel) QueueUnbind(name, key, exchange string, args amqp.Table) error {
	ch.mtx.RLock()
	defer ch.mtx.RUnlock()

	return ch.channel.QueueUnbind(name, key, exchange, args)
}

func (ch *Channel) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	ch.mtx.RLock()
	defer ch.mtx.RUnlock()

	return ch.channel.QueueDelete(name, ifUnused, ifEmpty, noWait)
}

func (ch *Channel) ConsumeWithContext(
	ctx context.Context,
	queue string,
	consumer string,
	autoAck bool,
	exclusive bool,
	noLocal bool,
	noWait bool,
	args amqp.Table,
) (<-chan amqp.Delivery, error) {
	return ch.consume(ctx, queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

func (ch *Channel) Consume(
	queue string,
	consumer string,
	autoAck bool,
	exclusive bool,
	noLocal bool,
	noWait bool,
	args amqp.Table,
) (<-chan amqp.Delivery, error) {
	return ch.consume(context.Background(), queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

func (ch *Channel) consume(
	ctx context.Context,
	queue string,
	consumer string,
	autoAck bool,
	exclusive bool,
	noLocal bool,
	noWait bool,
	args amqp.Table,
) (<-chan amqp.Delivery, error) {
	deliveryCh := make(chan amqp.Delivery, 1)

	go func() {
		for {
			ch.mtx.RLock()
			deliveries, err := ch.channel.ConsumeWithContext(
				ctx,
				queue,
				consumer,
				autoAck,
				exclusive,
				noLocal,
				noWait,
				args,
			)
			ch.mtx.RUnlock()

			if err != nil {
				ch.logger.Error(fmt.Sprintf("Failed to consume: %s", err.Error()))
				time.Sleep(ch.reconnectDelay)
				continue
			}

			for msg := range deliveries {
				deliveryCh <- msg
			}

			if ch.IsClosed() {
				break
			}

			time.Sleep(ch.reconnectDelay)
		}
	}()

	return deliveryCh, nil
}

func (ch *Channel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	ch.mtx.RLock()
	defer ch.mtx.RUnlock()

	return ch.channel.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
}
