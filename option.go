package zkclient

import (
	"time"

	"github.com/funkygao/go-zookeeper/zk"
)

type clientOption func(*client)

func WithSessionTimeout(sessionTimeout time.Duration) clientOption {
	return func(c *client) {
		c.sessionTimeout = sessionTimeout
	}
}

func WithAcl(acl []zk.ACL) clientOption {
	return func(c *client) {
		c.acl = acl
	}
}
