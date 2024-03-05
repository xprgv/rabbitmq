package unsafe

import (
	"fmt"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/xprgv/rabbitmq/pkg/logger"
)

type Connection struct {
	*amqp.Connection

	closed *atomic.Bool

	logger         logger.Logger
	reconnectDelay time.Duration
	onConnect      func()
	onDisconnect   func(reason *amqp.Error)
}

func DialConfig(url string, config amqp.Config, options ...Option) (*Connection, error) {
	opts := getDefaultOptions()

	for _, opt := range options {
		if opt != nil {
			if err := opt(&opts); err != nil {
				return nil, err
			}
		}
	}

	c, err := amqp.DialConfig(url, config)
	if err != nil {
		return nil, err
	}

	go opts.OnConnect()

	conn := &Connection{
		Connection: c,

		closed: &atomic.Bool{},

		logger:         opts.Logger,
		reconnectDelay: opts.ReconnectDelay,
		onConnect:      opts.OnConnect,
		onDisconnect:   opts.OnDisconnect,
	}

	go func() {
		for {
			reason, ok := <-conn.NotifyClose(make(chan *amqp.Error))

			go conn.onDisconnect(reason)

			if !ok || conn.IsClosed() {
				break
			}

			conn.logger.Error(fmt.Sprintf("Connection closed. Reason: %s", reason.Error()))

			for {
				time.Sleep(conn.reconnectDelay)

				c, err := amqp.DialConfig(url, config)
				if err != nil {
					conn.logger.Warn(fmt.Sprintf("Failed to reconnect. Reason: %s", err.Error()))
					continue
				}

				conn.Connection = c
				go conn.onConnect()

				conn.logger.Info("Successfully reconnected to rabbitmq")
				break
			}
		}
	}()

	return conn, nil
}

func (c *Connection) IsClosed() bool { return c.closed.Load() }

func (c *Connection) Close() error {
	if c.IsClosed() {
		return amqp.ErrClosed
	}

	c.closed.Store(true)

	return c.Connection.Close()
}

func (c *Connection) Channel() (*Channel, error) {
	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, err
	}

	channel := &Channel{
		Channel:        ch,
		closed:         &atomic.Bool{},
		logger:         c.logger,
		reconnectDelay: c.reconnectDelay,
	}

	go func() {
		for {
			reason, ok := <-channel.NotifyClose(make(chan *amqp.Error))
			if !ok || channel.IsClosed() {
				_ = channel.Close()
				break
			}

			c.logger.Error(fmt.Sprintf("Channel closed. Reason: %s", reason))

			for {
				time.Sleep(channel.reconnectDelay)

				ch, err := c.Connection.Channel()
				if err != nil {
					c.logger.Error(fmt.Sprintf("Failed to recreate channel, err: %s", err.Error()))
					continue
				}

				c.logger.Info("Channel successfully reconnected")

				channel.Channel = ch

				break
			}
		}
	}()

	return channel, nil
}
