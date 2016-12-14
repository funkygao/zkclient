package zkclient

import (
	"testing"
	"time"

	"github.com/funkygao/assert"
	"github.com/funkygao/go-zookeeper/zk"
)

func TestAllOptions(t *testing.T) {
	zkSvr := "localhost:2181"

	c := New(zkSvr, WithoutRetry())
	assert.Equal(t, false, c.withRetry)

	c = New(zkSvr, WithRetryAttempts(5))
	assert.Equal(t, true, c.withRetry)
	assert.Equal(t, 5, zkRetryOptions.MaxAttempts)

	c = New(zkSvr, WithRetryBackoff(time.Second*13))
	assert.Equal(t, time.Second*13, zkRetryOptions.Backoff)
	assert.Equal(t, true, c.withRetry)

	c = New(zkSvr, WithSessionTimeout(time.Millisecond))
	assert.Equal(t, time.Millisecond, c.sessionTimeout)

	c = New(zkSvr, WithWrapErrorWithPath())
	assert.Equal(t, true, c.wrapErrorWithPath)

	c = New(zkSvr, WithRetryLog(true))
	assert.Equal(t, true, zkRetryOptions.UseV1Info)

	c = New(zkSvr, WithACL([]zk.ACL{}))
	assert.Equal(t, []zk.ACL{}, c.acl)
}
