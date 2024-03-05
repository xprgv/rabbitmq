package unsafe

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/xprgv/rabbitmq/pkg/logger"
)

type Options struct {
	Logger         logger.Logger
	ReconnectDelay time.Duration
	OnConnect      func()
	OnDisconnect   func(reason *amqp.Error)
}

func getDefaultOptions() Options {
	return Options{
		Logger:         logger.Null{},
		ReconnectDelay: 500 * time.Millisecond,
		OnConnect:      func() {},
		OnDisconnect:   func(_ *amqp.Error) {},
	}
}

type Option func(*Options) error

func WithOnConnect(f func()) Option {
	return func(o *Options) error {
		if f == nil {
			f = func() {}
		}
		o.OnConnect = f
		return nil
	}
}

func WithOnDisconnect(f func(reason *amqp.Error)) Option {
	return func(o *Options) error {
		if f == nil {
			f = func(_ *amqp.Error) {}
		}
		o.OnDisconnect = f
		return nil
	}
}

func WithReconnectDelay(d time.Duration) Option {
	return func(o *Options) error {
		o.ReconnectDelay = d
		return nil
	}
}

func WithLogger(l logger.Logger) Option {
	return func(o *Options) error {
		if l == nil {
			l = logger.Null{}
		}
		o.Logger = l
		return nil
	}
}
