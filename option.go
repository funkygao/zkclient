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
