package zkclient

import (
	"time"

	"github.com/funkygao/go-zookeeper/zk"
)

type Option func(*Client)

func WithSessionTimeout(sessionTimeout time.Duration) Option {
	return func(c *Client) {
		c.sessionTimeout = sessionTimeout
	}
}

func WithACL(acl []zk.ACL) Option {
	return func(c *Client) {
		c.acl = acl
	}
}

func WithRetryAttempts(attempts int) Option {
	return func(*Client) {
		zkRetryOptions.MaxAttempts = attempts
	}
}

func WithRetryLog(useV1Info bool) Option {
	return func(*Client) {
		zkRetryOptions.UseV1Info = useV1Info
	}
}

func WithRetryBackoff(backoff time.Duration) Option {
	return func(*Client) {
		zkRetryOptions.Backoff = backoff
	}
}
