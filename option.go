package zkclient

import (
	"time"
)

type clientOption func(*client)

func WithSessionTimeout(sessionTimeout time.Duration) clientOption {
	return func(c *client) {
		c.sessionTimeout = sessionTimeout
	}
}
