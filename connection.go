package rabbitmq

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/xprgv/rabbitmq/pkg/logger"
)

type Connection struct {
	connection *amqp.Connection

	mtx            sync.RWMutex
	closed         *atomic.Bool
	logger         logger.Logger
	reconnectDelay time.Duration
}

func DialConfig(url string, config amqp.Config, log logger.Logger, reconnectDelay time.Duration) (*Connection, error) {
	c, err := amqp.DialConfig(url, config)
	if err != nil {
		return nil, err
	}

	if log == nil {
		log = logger.Null{}
	}

	if reconnectDelay == 0 {
		reconnectDelay = 100 * time.Millisecond
	}

	conn := &Connection{
		connection:     c,
		mtx:            sync.RWMutex{},
		closed:         &atomic.Bool{},
		logger:         log,
		reconnectDelay: reconnectDelay,
	}

	go func() {
		for {
			reason, ok := <-conn.NotifyClose(make(chan *amqp.Error))
			if !ok || conn.IsClosed() {
				conn.logger.Debug("Connection closed")
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

				conn.mtx.Lock()
				conn.connection = c
				conn.mtx.Unlock()

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

	c.mtx.RLock()
	defer c.mtx.RUnlock()

	return c.connection.Close()
}

func (c *Connection) LocalAddr() net.Addr {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	return c.connection.LocalAddr()
}

func (c *Connection) RemoteAddr() net.Addr {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	return c.connection.RemoteAddr()
}

func (c *Connection) NotifyClose(receiver chan *amqp.Error) chan *amqp.Error {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	return c.connection.NotifyClose(receiver)
}

func (c *Connection) Channel() (*Channel, error) {
	ch, err := c.connection.Channel()
	if err != nil {
		return nil, err
	}

	channel := &Channel{
		channel:        ch,
		mtx:            sync.RWMutex{},
		closed:         &atomic.Bool{},
		logger:         c.logger,
		reconnectDelay: c.reconnectDelay,
	}

	go func() {
		for {
			reason, ok := <-channel.NotifyClose(make(chan *amqp.Error))
			if !ok || channel.IsClosed() {
				channel.logger.Debug("Channel closed")
				_ = channel.Close()
				break
			}

			channel.logger.Error(fmt.Sprintf("Channel closed. Reason: %s", reason))

			for {
				time.Sleep(channel.reconnectDelay)

				c.mtx.RLock()
				ch, err := c.connection.Channel()
				c.mtx.RUnlock()

				if err != nil {
					c.logger.Error(fmt.Sprintf("Failed to recreate channel, err: %s", err.Error()))
					continue
				}

				channel.logger.Info("Channel successfully reconnected")

				channel.mtx.Lock()
				channel.channel = ch
				channel.mtx.Unlock()

				break
			}
		}
	}()

	return channel, nil
}
