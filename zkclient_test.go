package zkclient

import (
	"fmt"
	"io/ioutil"
	"log"
	"path"
	"testing"
	"time"

	"github.com/funkygao/assert"
	"github.com/funkygao/go-zookeeper/zk"
	glog "github.com/funkygao/log4go"
)

var (
	testZkSvr       = "localhost:2181"
	testZkSvrChroot = "localhost:2181/chroot"
)

func init() {
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
		glog.Disable()
	}

}

var _ ZkStateListener = &dummyListener{}
var _ ZkDataListener = &dummyListener{}
var _ ZkChildListener = &dummyListener{}

type dummyListener struct {
	t *testing.T

	childChanged  bool
	dataChanged   bool
	dataDeleted   bool
	stateChanged  bool
	hasNewSession bool
}

func (l *dummyListener) HandleStateChanged(state zk.State) error {
	l.t.Logf("new state %s", state)
	l.stateChanged = true
	return nil
}

func (l *dummyListener) HandleNewSession() error {
	l.t.Logf("new session")
	l.hasNewSession = true
	return nil
}

func (l *dummyListener) HandleChildChange(parentPath string, currentChilds []string) error {
	l.t.Logf("child change: %s %+v", parentPath, currentChilds)
	glog.Info("child change: %s %+v", parentPath, currentChilds)
	l.childChanged = true
	return nil
}

func (l *dummyListener) HandleDataChange(dataPath string, data []byte) error {
	l.t.Logf("data change: %s %s", dataPath, string(data))
	l.dataChanged = true
	glog.Info("%s data changed to %s", dataPath, string(data))
	return nil
}

func (l *dummyListener) HandleDataDeleted(dataPath string) error {
	l.t.Logf("data deleted: %s", dataPath)
	l.dataDeleted = true
	glog.Info("%s deleted", dataPath)
	return nil
}

func TestConnectionWithChroot(t *testing.T) {
	t.SkipNow() // TODO
}

func TestRealPath(t *testing.T) {
	c := New(testZkSvr)
	c.chroot = "/abc/efg"
	assert.Equal(t, "/abc/efg/mm", c.realPath("/mm"))
	assert.Equal(t, "/abc/efg/foo/bar", c.realPath("/foo/bar"))
	assert.Equal(t, "/abc/efg/foo/bar", c.realPath("/foo/bar/"))
	assert.Equal(t, "/abc/efg/foo/bar", c.realPath("foo/bar/"))
}

func TestWrapZkError(t *testing.T) {
	c := New(testZkSvr)
	assert.Equal(t, nil, c.wrapZkError("path", nil))
	assert.Equal(t, zk.ErrNothing, c.wrapZkError("path", zk.ErrNothing))
	c = New(testZkSvr, WithWrapErrorWithPath())
	assert.Equal(t, nil, c.wrapZkError("/foo/bar", nil))
	err := c.wrapZkError("/foo/bar", zk.ErrNodeExists)
	assert.Equal(t, "/foo/bar zk: node already exists", err.Error())
	lastErrInfo := err.Error()

	// wrap for only 1 level
	err = c.wrapZkError("/new/path/", err)
	assert.Equal(t, lastErrInfo, err.Error())
}

func TestZkErrorCompare(t *testing.T) {
	c := New(testZkSvr)
	c.wrapErrorWithPath = false
	assert.Equal(t, false, c.isZkError(nil, nil))
	assert.Equal(t, true, c.isZkError(c.wrapZkError("path", zk.ErrNodeExists), zk.ErrNodeExists))
	c.wrapErrorWithPath = true
	assert.Equal(t, true, c.isZkError(c.wrapZkError("path", zk.ErrNodeExists), zk.ErrNodeExists))
}

func TestWalk(t *testing.T) {
	c := New(testZkSvr)
	c.SetSessionTimeout(time.Second * 41)
	err := c.Connect()
	assert.Equal(t, nil, err)
	err = c.Walk("/", func(path string, stat *zk.Stat, err error) error {
		t.Logf("%s %v", path, err)
		return nil
	})
	assert.Equal(t, nil, err)

	c.Disconnect()
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

func TestChildrenValues(t *testing.T) {
	c := New(testZkSvr)
	c.SetSessionTimeout(time.Second * 41)
	err := c.Connect()
	assert.Equal(t, nil, err)

	now := time.Now()
	root := "/TestChildrenValues" + now.Format("20060102150405")
	defer func() {
		c.DeleteTree(root)
		c.Disconnect()
	}()

	data := []byte("hello world")
	assert.Equal(t, nil, c.CreatePersistent(root, data))
	assert.Equal(t, nil, c.CreatePersistent(path.Join(root, "a"), data))
	assert.Equal(t, nil, c.CreatePersistent(path.Join(root, "b"), data))

	chilren, values, err := c.ChildrenValues(root)
	assert.Equal(t, nil, err)
	assert.Equal(t, data, values[1])
	assert.Equal(t, 2, len(values))
	t.Logf("%+v %+v", chilren, values)
}

func TestMiltipleConnect(t *testing.T) {
	c := New(testZkSvr)
	for i := 0; i < 5; i++ {
		assert.Equal(t, nil, c.Connect())
		c.Disconnect()
	}
}

func TestConnectSubscribeStateChanges(t *testing.T) {
	c := New(testZkSvr)
	l := &dummyListener{t: t}
	// subscribe before connect
	c.SubscribeStateChanges(l)
	err := c.Connect()
	assert.Equal(t, nil, err)
	c.WaitUntilConnected(0)
	assert.Equal(t, true, l.hasNewSession)
	assert.Equal(t, true, l.stateChanged)
	c.Disconnect()
}

func TestSubscribeDataChanges(t *testing.T) {
	c := New(testZkSvr)
	l := &dummyListener{t: t}

	now := time.Now()
	path := "/TestSubscribeDataChanges" + now.Format("20060102150405")

	glog.Info("connecting")
	c.SubscribeDataChanges(path, l)

	// test duplicated subscribe
	for i := 0; i < 5; i++ {
		c.SubscribeDataChanges(path, l)
	}

	err := c.Connect()
	assert.Equal(t, nil, err)
	defer func() {
		glog.Info("disconnecting...")
		c.Disconnect()
	}()

	assert.Equal(t, nil, c.CreatePersistent(path, []byte{}))
	defer func() {
		glog.Info("deleting %s", path)
		c.DeleteTree(path)
	}()
	glog.Info("%s created", path)

	// trigger data change
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, nil, c.Set(path, []byte("haha")))
	glog.Info("%s updated", path)

	time.Sleep(time.Second)
	assert.Equal(t, true, l.dataChanged)
}

func TestUnsubscribeDataChanges(t *testing.T) {
	c := New(testZkSvr)
	l := &dummyListener{t: t}

	now := time.Now()
	path := "/TestSubscribeDataChanges" + now.Format("20060102150405")

	glog.Info("connecting")
	c.SubscribeDataChanges(path, l)

	// test duplicated subscribe
	for i := 0; i < 5; i++ {
		c.SubscribeDataChanges(path, l)
	}

	err := c.Connect()
	assert.Equal(t, nil, err)
	defer func() {
		glog.Info("disconnecting...")
		c.Disconnect()
	}()

	assert.Equal(t, nil, c.CreatePersistent(path, []byte{}))
	defer func() {
		glog.Info("deleting %s", path)
		c.DeleteTree(path)
	}()
	glog.Info("%s created", path)

	for i := 0; i < 3; i++ {
		c.UnsubscribeDataChanges(path, l)
	}

	time.Sleep(time.Second)
}

func TestSubscribeChildChanges(t *testing.T) {
	c := New(testZkSvr)
	l := &dummyListener{t: t}

	now := time.Now()
	path := "/TestSubscribeChildChanges" + now.Format("20060102150405")

	glog.Info("connecting")
	c.SubscribeChildChanges(path, l)

	// test duplicated subscribe
	for i := 0; i < 10; i++ {
		c.SubscribeChildChanges(path, l)
	}

	err := c.Connect()
	assert.Equal(t, nil, err)
	defer func() {
		glog.Info("disconnecting...")
		c.Disconnect()
	}()

	assert.Equal(t, nil, c.CreatePersistent(path, []byte{}))
	defer func() {
		glog.Info("deleting %s", path)
		c.DeleteTree(path)
	}()
	glog.Info("%s created", path)

	// trigger child change
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, nil, c.CreatePersistent(path+"/mmm", []byte{}))
	glog.Info("%s created", path+"/mmm")

	time.Sleep(time.Second)
	assert.Equal(t, true, l.childChanged)
}

func TestUnsubscribeChildChanges(t *testing.T) {
	c := New(testZkSvr)
	l := &dummyListener{t: t}

	now := time.Now()
	path := "/TestSubscribeChildChanges" + now.Format("20060102150405")

	glog.Info("connecting")
	c.SubscribeChildChanges(path, l)

	// test duplicated subscribe
	for i := 0; i < 10; i++ {
		c.SubscribeChildChanges(path, l)
	}

	err := c.Connect()
	assert.Equal(t, nil, err)
	defer func() {
		glog.Info("disconnecting...")
		c.Disconnect()
	}()

	c.UnsubscribeChildChanges(path, l)
	// permit multiple calls
	c.UnsubscribeChildChanges(path, l)

	time.Sleep(time.Second)
}

func TestSubscribeChildChangesFastChange(t *testing.T) {
	c := New(testZkSvr)
	l := &dummyListener{t: t}

	now := time.Now()
	path := "/TestSubscribeChildChanges" + now.Format("20060102150405")

	glog.Info("connecting")
	c.SubscribeChildChanges(path, l)

	err := c.Connect()
	assert.Equal(t, nil, err)
	defer func() {
		glog.Info("disconnecting...")
		c.Disconnect()
	}()

	assert.Equal(t, nil, c.CreatePersistent(path, []byte{}))
	defer func() {
		glog.Info("deleting %s", path)
		c.DeleteTree(path)
	}()
	glog.Info("%s created", path)

	// trigger child change very fast to check if watch loses event notification
	// YES, it loses!
	for i := 0; i < 10; i++ {
		assert.Equal(t, nil, c.CreatePersistent(path+"/mmm", []byte{}))
		glog.Info("%s created", path+"/mmm")

		assert.Equal(t, nil, c.Delete(path+"/mmm"))
		glog.Info("%s deleted", path+"/mmm")
	}

	time.Sleep(time.Second)
	assert.Equal(t, true, l.childChanged)
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

func TestChrootChildrenValues(t *testing.T) {
	c := New(testZkSvrChroot)
	c.SetSessionTimeout(time.Second * 41)
	c.withRetry = false
	err := c.Connect()
	assert.Equal(t, nil, err)

	now := time.Now()
	root := "/TestChildrenValues" + now.Format("20060102150405")
	defer func() {
		c.DeleteTree(root)
		c.Disconnect()
	}()

	data := []byte("hello world")
	assert.Equal(t, nil, c.CreatePersistent(root, data))
	assert.Equal(t, nil, c.CreatePersistent(path.Join(root, "a"), data))
	assert.Equal(t, nil, c.CreatePersistent(path.Join(root, "b"), data))

	chilren, values, err := c.ChildrenValues(root)
	assert.Equal(t, nil, err)
	assert.Equal(t, data, values[1])
	assert.Equal(t, 2, len(values))
	t.Logf("%+v %+v", chilren, values)
}
