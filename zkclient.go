package zkclient

import (
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/funkygao/go-zookeeper/zk"
	"github.com/funkygao/golib/sync2"
	log "github.com/funkygao/log4go"
	"github.com/yichen/retry"
)

type Client struct {
	sync.RWMutex

	zkSvr, chroot     string
	servers           []string
	sessionTimeout    time.Duration
	withRetry         bool
	wrapErrorWithPath bool

	isConnected   sync2.AtomicBool
	connectCalled chan struct{}
	close         chan struct{}
	wg            sync.WaitGroup

	zkConn     *zk.Conn
	stat       *zk.Stat // storage for the lastest zk query stat info FIXME
	stateEvtCh <-chan zk.Event
	acl        []zk.ACL

	lisenterErrCh chan error

	stateLock            sync.RWMutex
	stateChangeListeners []ZkStateListener

	childLock            sync.RWMutex
	childChangeListeners map[string][]ZkChildListener
	childWatchStopper    map[string]chan struct{}

	dataLock            sync.RWMutex
	dataChangeListeners map[string][]ZkDataListener
	dataWatchStopper    map[string]chan struct{}
}

var defaultSessionTimeout = time.Second * 30

// New will create a zookeeper Client.
func New(zkSvr string, options ...Option) *Client {
	servers, chroot, err := parseZkConnStr(zkSvr)
	if err != nil || len(servers) == 0 {
		// yes, panic!
		panic("invalid zkSvr")
	}

	c := &Client{
		zkSvr:                zkSvr,
		chroot:               chroot,
		servers:              servers,
		sessionTimeout:       defaultSessionTimeout,
		withRetry:            false, // without retry by default
		acl:                  zk.WorldACL(zk.PermAll),
		wrapErrorWithPath:    false,
		stateChangeListeners: []ZkStateListener{},
		childChangeListeners: map[string][]ZkChildListener{},
		childWatchStopper:    map[string]chan struct{}{},
		dataChangeListeners:  map[string][]ZkDataListener{},
		dataWatchStopper:     map[string]chan struct{}{},
		lisenterErrCh:        make(chan error, 1<<8),
		connectCalled:        make(chan struct{}),
	}
	c.isConnected.Set(false)

	for _, option := range options {
		option(c)
	}

	return c
}

// Connect will connect to zookeeper ensemble.
func (c *Client) Connect() error {
	t1 := time.Now()

	zkConn, stateEvtCh, err := zk.Connect(c.servers, c.sessionTimeout)
	if err != nil {
		return err
	}

	close(c.connectCalled)
	c.close = make(chan struct{})
	c.zkConn = zkConn
	c.stateEvtCh = stateEvtCh

	if c.chroot != "" {
		if err := c.ensurePathExists(c.chroot); err != nil {
			return err
		}
	}

	// always watch state changes to maintain IsConnected
	c.wg.Add(1)
	go c.watchStateChanges()

	log.Debug("zkClient Connect %s", time.Since(t1))

	return nil
}

// Disconnect will disconnect from the zookeeper ensemble and release related resources.
func (c *Client) Disconnect() {
	t1 := time.Now()

	if c.zkConn != nil {
		c.zkConn.Close()
	}
	close(c.close)
	c.wg.Wait()

	c.zkConn = nil
	c.stat = nil
	c.isConnected.Set(false)

	log.Debug("zkClient Disconnect %s", time.Since(t1))
}

// ZkSvr returns the raw zookeeper servers connection string.
func (c *Client) ZkSvr() string {
	return c.zkSvr
}

func (c *Client) SessionTimeout() time.Duration {
	return c.sessionTimeout
}

func (c *Client) SetSessionTimeout(t time.Duration) error {
	if c.IsConnected() {
		return ErrNotAllowed
	}

	c.sessionTimeout = t
	return nil
}

// SubscribeStateChanges MUST be called before Connect as we don't want
// to labor to handle the thread-safe issue.
func (c *Client) SubscribeStateChanges(listener ZkStateListener) {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()

	ok := true
	for _, l := range c.stateChangeListeners {
		if l == listener {
			log.Warn("duplicated state changes subscribe %p", listener)
			ok = false
			break
		}
	}
	if ok {
		c.stateChangeListeners = append(c.stateChangeListeners, listener)
	}
}

func (c *Client) watchStateChanges() {
	defer c.wg.Done()

	var (
		evt   zk.Event
		err   error
		loops int
	)
	for {
		loops++

		select {
		case <-c.close:
			log.Debug("#%d got close signal, quit", loops)
			return

		case evt = <-c.stateEvtCh:
			// TODO what if handler blocks?
			c.stateLock.Lock()
			for _, l := range c.stateChangeListeners {
				if err = l.HandleStateChanged(evt.State); err != nil {
					log.Error("#%d %v", loops, err)
				}
			}

			// extra handler for new session state
			if evt.State == zk.StateHasSession {
				c.isConnected.Set(true)
				for _, l := range c.stateChangeListeners {
					if err = l.HandleNewSession(); err != nil {
						log.Error("#%d %v", loops, err)
					}
				}
			} else if evt.State != zk.StateUnknown {
				c.isConnected.Set(false)
			}
			c.stateLock.Unlock()
		}
	}
}

func (c *Client) SubscribeChildChanges(path string, listener ZkChildListener) {
	c.childLock.Lock()
	defer c.childLock.Unlock()

	startWatch := false
	if _, present := c.childChangeListeners[path]; !present {
		c.childChangeListeners[path] = []ZkChildListener{}
		startWatch = true
	}

	ok := true
	for _, l := range c.childChangeListeners[path] {
		if l == listener {
			log.Warn("duplicated child changes subscribe: %s %p", path, listener)
			ok = false
			break
		}
	}
	if ok {
		c.childChangeListeners[path] = append(c.childChangeListeners[path], listener)
	}

	if startWatch {
		c.childWatchStopper[path] = make(chan struct{})

		c.wg.Add(1)
		go c.watchChildChanges(path)
	}
}

func (c *Client) UnsubscribeChildChanges(path string, listener ZkChildListener) {
	c.childLock.Lock()
	defer c.childLock.Unlock()

	var newListeners []ZkChildListener
	found := false
	for _, l := range c.childChangeListeners[path] {
		if l != listener {
			newListeners = append(newListeners, l)
		} else {
			found = true
		}
	}
	if found && len(newListeners) == 0 {
		close(c.childWatchStopper[path])
	}
	c.childChangeListeners[path] = newListeners
}

func (c *Client) watchChildChanges(path string) {
	defer c.wg.Done()

	if err := c.WaitUntilConnected(0); err != nil {
		log.Error("give up for %v", err)
		return
	}

	c.childLock.RLock()
	stopper := c.childWatchStopper[path]
	c.childLock.RUnlock()

	log.Trace("start watching %s child changes", path)
	var loops int
	for {
		loops++

		select {
		case <-c.close:
			log.Debug("%s#%d yes sir, quit", path, loops)
			return
		case <-stopper:
			log.Debug("%s#%d yes sir, stopped", path, loops)
			return
		default:
		}

		// because zk watcher is one-time trigger, if the event happends too fast
		// we might lose events between the watchers:
		// watches are used to find out about the latest change
		currentChilds, evtCh, err := c.ChildrenW(path)
		if err != nil {
			switch err {
			case zk.ErrNoNode:
				// sleep blindly
				log.Trace("%s#%d %s, will retry", path, loops, err)
				time.Sleep(time.Millisecond * 100)

			case zk.ErrClosing:
				log.Trace("%s#%d zk closing", path, loops)
				return

			default:
				log.Error("%s#%d %v", path, loops, err)
			}

			c.fireListenerError(err)
			continue
		}

		log.Debug("%s#%d ok, waiting for child change event...", path, loops)
		select {
		case <-c.close:
			log.Debug("%s#%d yes sir, quit", path, loops)
			return

		case <-stopper:
			log.Debug("%s#%d yes sir, stopped", path, loops)
			return

		case evt, ok := <-evtCh:
			if !ok {
				log.Warn("%s#%d event channel closed, quit", path, loops)
				return
			}

			log.Debug("%s#%d got event %+v", path, loops, evt)

			if evt.Err != nil {
				log.Error("%s#%d", path, loops)
				c.fireListenerError(evt.Err)
				continue
			}
			if evt.Type != zk.EventNodeChildrenChanged {
				// ignore
				log.Debug("%s#%d ignored %+v", path, loops, evt)
				continue
			}

			c.childLock.Lock()
			log.Debug("%s#%d dispatching %+v to %d listeners", path, loops, currentChilds, len(c.childChangeListeners[path]))
			for _, l := range c.childChangeListeners[path] {
				if err = l.HandleChildChange(path, currentChilds); err != nil {
					log.Error("%s#%d %+v %v", path, loops, currentChilds, err)
				}
			}
			c.childLock.Unlock()
		}
	}

}

func (c *Client) SubscribeDataChanges(path string, listener ZkDataListener) {
	c.dataLock.Lock()
	defer c.dataLock.Unlock()

	startWatch := false
	if _, present := c.dataChangeListeners[path]; !present {
		c.dataChangeListeners[path] = []ZkDataListener{}
		startWatch = true
	}

	ok := true
	for _, l := range c.dataChangeListeners[path] {
		if l == listener {
			log.Warn("duplicated data changes subscribe: %s %p", path, listener)
			ok = false
			break
		}
	}
	if ok {
		c.dataChangeListeners[path] = append(c.dataChangeListeners[path], listener)
	}

	if startWatch {
		c.dataWatchStopper[path] = make(chan struct{})

		c.wg.Add(1)
		go c.watchDataChanges(path)
	}
}

func (c *Client) watchDataChanges(path string) {
	defer c.wg.Done()

	if err := c.WaitUntilConnected(0); err != nil {
		log.Error("give up for %v", err)
		return
	}

	c.dataLock.RLock()
	stopper := c.dataWatchStopper[path]
	c.dataLock.RUnlock()

	log.Trace("start watching data changes: %s", path)
	var loops int
	for {
		loops++

		select {
		case <-c.close:
			log.Debug("%s#%d yes sir, quit", path, loops)
			return
		case <-stopper:
			log.Debug("%s#%d yes sir, stopped", path, loops)
			return
		default:
		}

		data, evtCh, err := c.GetW(path)
		if err != nil {
			switch err {
			case zk.ErrNoNode:
				// sleep blindly
				log.Trace("%s#%d %s, will retry", path, loops, err)
				time.Sleep(time.Millisecond * 100)

			case zk.ErrClosing:
				log.Trace("%s#%d zk closing", path, loops)
				return

			default:
				log.Error("%s#%d %v", path, loops, err)
			}

			c.fireListenerError(err)
			continue
		}

		log.Debug("%s#%d ok, waiting for data change event...", path, loops)
		select {
		case <-c.close:
			log.Debug("%s#%d yes sir, quit", path, loops)
			return

		case <-stopper:
			log.Debug("%s#%d yes sir, stopped", path, loops)
			return

		case evt, ok := <-evtCh:
			if !ok {
				log.Warn("%s#%d event channel closed, quit", path, loops)
				return
			}

			if evt.Err != nil {
				log.Error("%s#%d", path, loops)
				c.fireListenerError(evt.Err)
				continue
			}
			if evt.Type != zk.EventNodeDataChanged && evt.Type != zk.EventNodeDeleted {
				// ignore
				log.Debug("%s#%d ignored %+v", path, loops, evt)
				continue
			}

			log.Debug("%s#%d got event %+v", path, loops, evt)

			c.childLock.Lock()
			for _, l := range c.dataChangeListeners[path] {
				switch evt.Type {
				case zk.EventNodeDataChanged:
					if err = l.HandleDataChange(path, data); err != nil {
						log.Error("%s#%d %v", path, loops, err)
					}

				case zk.EventNodeDeleted:
					if err = l.HandleDataDeleted(path); err != nil {
						log.Error("%s#%d %v", path, loops, err)
					}
				}
			}
			c.childLock.Unlock()
		}
	}

}

func (c *Client) UnsubscribeDataChanges(path string, listener ZkDataListener) {
	c.dataLock.Lock()
	defer c.dataLock.Unlock()

	var newListeners []ZkDataListener
	found := false
	for _, l := range c.dataChangeListeners[path] {
		if l != listener {
			newListeners = append(newListeners, l)
		} else {
			found = true
		}
	}
	if found && len(newListeners) == 0 {
		close(c.dataWatchStopper[path])
	}
	c.dataChangeListeners[path] = newListeners
}

func (c *Client) realPath(path string) string {
	if c.chroot == "" {
		return path
	}

	return strings.TrimRight(c.chroot+path, "/")
}

func (c *Client) fireListenerError(err error) {
	select {
	case c.lisenterErrCh <- err:
	default:
		// discard silently
	}
}

// LastStat returns last read operation(Exists/Get/GetW/Children/ChildrenW) zk stat result.
func (c *Client) LastStat() *zk.Stat {
	return c.stat
}

// WaitUntilConnected will block till zookeeper ensemble is really connected.
// If arg d is 0, means infinite timeout.
func (c *Client) WaitUntilConnected(d time.Duration) (err error) {
	t1 := time.Now()
	retries := 0
	for {
		<-c.connectCalled

		if _, _, err = c.zkConn.Exists("/zookeeper"); err == nil {
			break
		}

		retries++
		log.Debug("WaitUntilConnected: retry=%d %v", retries, err)

		if d > 0 && time.Since(t1) > d {
			break
		} else if d > 0 {
			select {
			case <-c.close:
				return
			case <-time.After(d): // TODO time wheel
			}
		} else {
			select {
			case <-c.close:
				return
			case <-time.After(c.sessionTimeout):
			}
		}
	}

	log.Debug("zkClient WaitUntilConnected %s", time.Since(t1))
	return
}

func (c *Client) IsConnected() bool {
	return c != nil && c.isConnected.Get()
}

func (c *Client) SessionID() string {
	return strconv.FormatInt(c.zkConn.SessionID(), 10)
}

func (c *Client) Exists(path string) (result bool, err error) {
	if c.withRetry {
		err = retry.RetryWithBackoff(zkRetryOptions, func() (retry.RetryStatus, error) {
			result, c.stat, err = c.zkConn.Exists(c.realPath(path))
			if err != nil {
				return retry.RetryContinue, c.wrapZkError(path, err)
			}

			return retry.RetryBreak, nil
		})
	} else {
		result, c.stat, err = c.zkConn.Exists(c.realPath(path))
	}

	return
}

func (c *Client) ExistsAll(paths ...string) (bool, error) {
	for _, path := range paths {
		if exists, err := c.Exists(path); err != nil || exists == false {
			return exists, err
		}
	}

	return true, nil
}

func (c *Client) Get(path string) (data []byte, err error) {
	if c.withRetry {
		err = retry.RetryWithBackoff(zkRetryOptions, func() (retry.RetryStatus, error) {
			data, c.stat, err = c.zkConn.Get(c.realPath(path))
			if err != nil {
				return retry.RetryContinue, c.wrapZkError(path, err)
			}

			return retry.RetryBreak, nil
		})
	} else {
		data, c.stat, err = c.zkConn.Get(c.realPath(path))
	}

	return
}

func (c *Client) GetW(path string) (data []byte, events <-chan zk.Event, err error) {
	if c.withRetry {
		err = retry.RetryWithBackoff(zkRetryOptions, func() (retry.RetryStatus, error) {
			data, c.stat, events, err = c.zkConn.GetW(c.realPath(path))
			if err != nil {
				return retry.RetryContinue, c.wrapZkError(path, err)
			}

			return retry.RetryBreak, nil
		})
	} else {
		data, c.stat, events, err = c.zkConn.GetW(c.realPath(path))
	}

	return
}

func (c *Client) Set(path string, data []byte) error {
	_, err := c.zkConn.Set(c.realPath(path), data, c.stat.Version)
	return err
}

func (c *Client) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	return c.zkConn.Create(c.realPath(path), data, flags, acl)
}

func (c *Client) CreatePersistent(path string, data []byte) error {
	flags := int32(0)
	_, err := c.Create(path, data, flags, c.acl)
	return err
}

func (c *Client) CreateEmptyPersistent(path string) error {
	return c.CreatePersistent(path, []byte{})
}

func (c *Client) CreateEphemeral(path string, data []byte) error {
	flags := int32(zk.FlagEphemeral)
	_, err := c.Create(path, data, flags, c.acl)
	return err
}

func (c *Client) CreatePersistentRecord(p string, r Marshaller) error {
	parent := path.Dir(p)
	err := c.ensurePathExists(c.realPath(parent))
	if err != nil {
		return err
	}

	return c.CreatePersistent(p, r.Marshal())
}

func (c *Client) SetRecord(path string, r Marshaller) error {
	exists, err := c.Exists(path)
	if err != nil {
		return err
	}

	if !exists {
		if err = c.ensurePathExists(c.realPath(path)); err != nil {
			return err
		}
	}

	if _, err = c.Get(path); err != nil {
		return err
	}

	return c.Set(path, r.Marshal())
}

func (c *Client) Children(path string) (children []string, err error) {
	if c.withRetry {
		err = retry.RetryWithBackoff(zkRetryOptions, func() (retry.RetryStatus, error) {
			children, c.stat, err = c.zkConn.Children(c.realPath(path))
			if err != nil {
				return retry.RetryContinue, c.wrapZkError(path, err)
			}

			return retry.RetryBreak, nil
		})
	} else {
		children, c.stat, err = c.zkConn.Children(c.realPath(path))
	}

	return
}

func (c *Client) ChildrenW(path string) (children []string, eventChan <-chan zk.Event, err error) {
	if c.withRetry {
		err = retry.RetryWithBackoff(zkRetryOptions, func() (retry.RetryStatus, error) {
			children, c.stat, eventChan, err = c.zkConn.ChildrenW(c.realPath(path))
			if err != nil {
				return retry.RetryContinue, c.wrapZkError(path, err)
			}

			return retry.RetryBreak, nil
		})
	} else {
		children, c.stat, eventChan, err = c.zkConn.ChildrenW(c.realPath(path))
	}

	return
}

func (c *Client) Delete(path string) error {
	return c.zkConn.Delete(c.realPath(path), -1)
}

func (c *Client) DeleteTree(path string) error {
	return c.deleteTreeRealPath(c.realPath(path))
}

func (c *Client) deleteTreeRealPath(path string) error {
	if exists, _, err := c.zkConn.Exists(path); !exists || err != nil {
		return err
	}

	children, _, err := c.zkConn.Children(path)
	if err != nil {
		return err
	}

	if len(children) == 0 {
		err := c.zkConn.Delete(path, -1)
		return err
	}

	for _, child := range children {
		p := path + "/" + child
		e := c.deleteTreeRealPath(p)
		if e != nil {
			return e
		}
	}

	return c.zkConn.Delete(path, -1)
}

func (c *Client) ensurePathExists(p string) error {
	if exists, _, _ := c.zkConn.Exists(p); exists {
		return nil
	}

	parent := path.Dir(p)
	if exists, _, _ := c.zkConn.Exists(parent); !exists {
		if err := c.ensurePathExists(parent); err != nil {
			return err
		}
	}

	flags := int32(0)
	c.zkConn.Create(p, []byte{}, flags, c.acl)
	return nil
}

func (c *Client) wrapZkError(path string, err error) error {
	if !c.wrapErrorWithPath {
		return err
	}

	return fmt.Errorf("%s %v", c.realPath(path), err)
}
