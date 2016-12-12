package zkclient

import (
	"github.com/funkygao/go-zookeeper/zk"
)

type ZkStateListener interface {
	HandleStateChanged(zk.State) error

	HandleNewSession() error
}
