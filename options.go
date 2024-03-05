package rabbitmq

import (
	"time"

	"github.com/xprgv/rabbitmq/pkg/logger"
)

type Options struct {
	Logger         logger.Logger
	ReconnectDelay time.Duration
	OnConnect      func()
	OnDisconnect   func()
}

func getDefaultOptions() Options {
	return Options{
		Logger:         logger.Null{},
		ReconnectDelay: 500 * time.Millisecond,
		OnConnect:      func() {},
		OnDisconnect:   func() {},
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

func WithOnDisconnect(f func()) Option {
	return func(o *Options) error {
		if f == nil {
			f = func() {}
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
