package zkclient

import (
	"github.com/funkygao/go-zookeeper/zk"
)

// ZkStateListener can be registered through SubscribeStateChanges.
type ZkStateListener interface {

	// HandleStateChanged is called when the zookeeper connection state has changed.
	HandleStateChanged(zk.State) error

	// HandleNewSession is called after the zookeeper session has expired and a new session has been created.
	// You would have to re-create any ephemeral nodes and re-watch here.
	HandleNewSession() error
}

// ZkChildListener can be registered through TODO for listening on zk child changes for a given path.
//
// Note: Also this listener re-subscribes it watch for the path on each zk event (zk watches are one-timers) is is not
// guaranteed that events on the path are missing (see http://zookeeper.wiki.sourceforge.net/ZooKeeperWatches).
// An implementation of this interface should take that into account.
type ZkChildListener interface {

	// HandleChildChange is called when the children of the given path changed.
	HandleChildChange(parentPath string, lastChilds []string) error
}

// ZkDataListener can be registered through TODO for listening on zk data changes for a given path.
type ZkDataListener interface {
	HandleDataChange(dataPath string, data []byte) error
	HandleDataDeleted(dataPath string) error
}
