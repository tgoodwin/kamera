package client

import "time"

type Config struct {
	LogObjectSnapshots    bool
	visibilityDelayByKind map[string]time.Duration

	disableLogging bool
}

func NewConfig() *Config {
	return &Config{
		LogObjectSnapshots:    true,
		visibilityDelayByKind: make(map[string]time.Duration),
	}
}

type Option func(*Config)

func LogObjectSnapshots() Option {
	return func(o *Config) {
		o.LogObjectSnapshots = true
	}
}

func VisibilityDelay(kind string, duration time.Duration) Option {
	return func(o *Config) {
		if o.visibilityDelayByKind == nil {
			o.visibilityDelayByKind = make(map[string]time.Duration)
		}
		o.visibilityDelayByKind[kind] = duration
	}
}

func (c *Client) WithOptions(opts ...Option) *Client {
	if c.config == nil {
		c.config = &Config{}
	}
	for _, opt := range opts {
		opt(c.config)
	}
	return c
}
