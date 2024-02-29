package unsafe

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/xprgv/rabbitmq/pkg/logger"
)

type Channel struct {
	*amqp.Channel

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

	return ch.Channel.Close()
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
	deliveryChan := make(chan amqp.Delivery, 1)

	go func() {
		for {
			deliveries, err := ch.Channel.ConsumeWithContext(
				ctx,
				queue,
				consumer,
				autoAck,
				exclusive,
				noLocal,
				noWait,
				args,
			)
			if err != nil {
				ch.logger.Error(fmt.Sprintf("Failed to consume: %s", err.Error()))
				time.Sleep(ch.reconnectDelay)
				continue
			}

			for msg := range deliveries {
				deliveryChan <- msg
			}

			if ch.IsClosed() {
				break
			}

			time.Sleep(ch.reconnectDelay)
		}
	}()

	return deliveryChan, nil
}
