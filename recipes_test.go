package zkclient

import (
	"testing"
	"time"

	"github.com/funkygao/assert"
	"github.com/funkygao/go-zookeeper/zk"
)

func TestRecipeCreateLiveNode(t *testing.T) {
	c := New(testZkSvr, WithSessionTimeout(time.Second))
	assert.Equal(t, nil, c.Connect())
	c.WaitUntilConnected(0)

	now := time.Now()
	path := "/TestRecipeCreateLiveNode" + now.Format("20060102150405")
	data := []byte(c.SessionID())
	defer func() {
		c.Connect()
		c.DeleteTree(path)
	}()

	assert.Equal(t, nil, c.CreateLiveNode(path, data, 5))

	c.Disconnect()

	for i := 0; i < 5; i++ {
		c.Connect()
		assert.Equal(t, nil, c.CreateLiveNode(path, data, 5))
		c.Disconnect()
	}

	assert.Equal(t, nil, c.Connect())
	for i := 0; i < 2; i++ {
		err := c.CreateLiveNode(path, data, 2)
		assert.Equal(t, true, err == nil || err == zk.ErrNodeExists)
	}

}
