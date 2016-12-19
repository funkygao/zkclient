package zkclient

import (
	"bytes"
	gopath "path"
	"strconv"
	"time"

	"github.com/funkygao/go-zookeeper/zk"
	log "github.com/funkygao/log4go"
)

// CreateLiveNode handles https://issues.apache.org/jira/browse/ZOOKEEPER-1740.
// An ephemeral node may still exist even after its corresponding session has
// expired and CreateLiveNode solves this problem.
func (c *Client) CreateLiveNode(path string, data []byte, maxRetry int) (err error) {
	log.Debug("%s[%s] creating live node...", c.SessionID(), path)

	if err = c.ensurePathExists(c.realPath(gopath.Dir(path))); err != nil {
		return
	}

	// it is possible the live node still exists from last run
	// retry to wait for the zookeeper to remove the live node from previous session
	backoff := c.SessionTimeout() + time.Millisecond*50
	var retry int
	for retry = 0; retry < maxRetry; retry++ {
		err = c.CreateEphemeral(path, data)
		if err == nil {
			break
		} else if err == zk.ErrNodeExists {
			if curData, stat, er := c.zkConn.Get(path); er == zk.ErrNoNode {
				log.Debug("%s[%s] #%d live node is gone as we check it, retry creating live node",
					c.SessionID(), path, retry)
				continue // needn't sleep backoff
			} else {
				curSessionID := strconv.FormatInt(stat.EphemeralOwner, 10)
				if curSessionID == c.SessionID() {
					if !bytes.Equal(curData, data) {
						log.Debug("%s[%s] #%d overwrite data with same session id", c.SessionID(), path, retry)
						if err = c.Set(path, data); err != nil {
							log.Error("%s[%s] #%d failed to overwrite %v", c.SessionID(), path, err, retry)
						}
					}
				} else {
					log.Debug("%s[%s] #%d await previous session expire...", c.SessionID(), path, retry)
				}
			}
		}

		// wait for zookeeper remove the last run's ephemeral znode
		log.Debug("%s[%s] #%d backoff %s for %v", c.SessionID(), path, retry, backoff, err)
		time.Sleep(backoff)
	}

	if err == nil {
		log.Debug("%s[%s] #%d created live node", c.SessionID(), path, retry)
	}

	return
}
