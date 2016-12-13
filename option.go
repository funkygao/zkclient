package zkclient

import (
	"time"

	"github.com/funkygao/go-zookeeper/zk"
)

type clientOption func(*Client)

func WithSessionTimeout(sessionTimeout time.Duration) clientOption {
	return func(c *Client) {
		c.sessionTimeout = sessionTimeout
	}
}

func WithACL(acl []zk.ACL) clientOption {
	return func(c *Client) {
		c.acl = acl
	}
}

func WithRetryAttempts(attempts int) clientOption {
	return func(*Client) {
		zkRetryOptions.MaxAttempts = attempts
	}
}

func WithRetryLog(useV1Info bool) clientOption {
	return func(*Client) {
		zkRetryOptions.UseV1Info = useV1Info
	}
}

func WithRetryBackoff(backoff time.Duration) clientOption {
	return func(*Client) {
		zkRetryOptions.Backoff = backoff
	}
}
