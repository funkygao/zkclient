package zkclient

import (
	"fmt"
	"io/ioutil"
	"log"
	"testing"
	"time"

	"github.com/funkygao/assert"
	"github.com/funkygao/go-zookeeper/zk"
	glog "github.com/funkygao/log4go"
)

var (
	testZkSvr = "localhost:2181"
)

func init() {
	log.SetOutput(ioutil.Discard)
	glog.Disable()
}

type dummyStateListener struct {
	t *testing.T
}

func (l *dummyStateListener) HandleStateChanged(state zk.State) error {
	l.t.Logf("new state %s", state)
	return nil
}

func (l *dummyStateListener) HandleNewSession() error {
	l.t.Logf("new session")
	return nil
}

func TestConnectionWithChroot(t *testing.T) {
	t.SkipNow() // TODO
}

func TestRealPath(t *testing.T) {
	c := New(testZkSvr)
	c.chroot = "/abc/efg"
	assert.Equal(t, "/abc/efg/mm", c.realPath("/mm"))
}

func TestConnectionWaitUntil(t *testing.T) {
	c := New(testZkSvr)
	c.SetSessionTimeout(time.Second * 41)
	err := c.Connect()
	assert.Equal(t, nil, err)
	assert.Equal(t, testZkSvr, c.ZkSvr())
	assert.Equal(t, time.Second*41, c.SessionTimeout())
	assert.Equal(t, nil, c.WaitUntilConnected(0))
	assert.Equal(t, ErrNotAllowed, c.SetSessionTimeout(time.Second))
	assert.Equal(t, time.Second*41, c.SessionTimeout())

	t1 := time.Now()
	err = c.WaitUntilConnected(0)
	assert.Equal(t, nil, err)
	t.Logf("waitUntilConnect %s", time.Since(t1))

	c.Disconnect()
}

func TestConnectSubscribeStateChanges(t *testing.T) {
	c := New(testZkSvr)
	l := &dummyStateListener{t: t}
	// subscribe before connect
	c.SubscribeStateChanges(l)
	err := c.Connect()
	assert.Equal(t, nil, err)
	c.WaitUntilConnected(0)
	c.Disconnect()
}

func TestConnectionEnsurePath(t *testing.T) {
	conn := New(testZkSvr)
	err := conn.Connect()
	assert.Equal(t, nil, err)
	defer conn.Disconnect()

	now := time.Now().Local()
	cluster := "zkclient_test_" + now.Format("20060102150405")
	p := fmt.Sprintf("/%s/a/b/c", cluster)

	err = conn.ensurePathExists(p)
	if err != nil {
		t.Error(err.Error())
	}

	exists, err := conn.Exists(p)
	assert.Equal(t, nil, err)
	assert.Equal(t, true, exists)

	root := "/" + cluster
	t.Logf("delete tree: %s", root)
	assert.Equal(t, nil, conn.DeleteTree(root))

	exists, err = conn.Exists(root)
	assert.Equal(t, nil, err)
	assert.Equal(t, false, exists)
}

func TestConnectionCRUD(t *testing.T) {
	c := New(testZkSvr)
	err := c.Connect()
	assert.Equal(t, nil, err)
	err = c.WaitUntilConnected(0)
	assert.Equal(t, nil, err)

	root := "/test_zk_connection"
	defer c.DeleteTree(root)

	assert.Equal(t, nil, c.CreateEphemeral(root, []byte{}))

	// TODO more test cases
}
