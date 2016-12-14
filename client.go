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

	isConnected sync2.AtomicBool
	close       chan struct{}
	wg          sync.WaitGroup

	zkConn     *zk.Conn
	stat       *zk.Stat // storage for the lastest zk query stat info FIXME
	stateEvtCh <-chan zk.Event
	acl        []zk.ACL

	stateChangeListeners []ZkStateListener
}

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
		close:                make(chan struct{}),
		sessionTimeout:       time.Second * 30,
		withRetry:            false, // without retry by default
		acl:                  zk.WorldACL(zk.PermAll),
		wrapErrorWithPath:    false,
		stateChangeListeners: []ZkStateListener{},
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

	c.zkConn = zkConn
	c.stateEvtCh = stateEvtCh

	if c.chroot != "" {
		if err := c.ensurePathExists(c.chroot); err != nil {
			return err
		}
	}

	c.wg.Add(1)
	go c.watchStateChanges()

	log.Debug("zk Client Connect %s", time.Since(t1))

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
	c.isConnected.Set(false)

	log.Debug("zk Client Disconnect %s", time.Since(t1))
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
	c.stateChangeListeners = append(c.stateChangeListeners, listener)
}

func (c *Client) watchStateChanges() {
	defer c.wg.Done()

	var evt zk.Event
	for {
		select {
		case <-c.close:
			log.Debug("zk Client got close signal, stopped ok")
			return

		case evt = <-c.stateEvtCh:
			// TODO lock? currently, SubscribeStateChanges must called before Connect
			// what if handler blocks?
			for _, l := range c.stateChangeListeners {
				l.HandleStateChanged(evt.State)
			}

			// extra handler for new session state
			if evt.State == zk.StateHasSession {
				c.isConnected.Set(true)
				for _, l := range c.stateChangeListeners {
					l.HandleNewSession()
				}
			} else if evt.State != zk.StateUnknown {
				c.isConnected.Set(false)
			}
		}
	}
}

func (c *Client) realPath(path string) string {
	if c.chroot == "" {
		return path
	}

	return strings.TrimRight(c.chroot+path, "/")
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
		if _, _, err = c.zkConn.Exists("/zookeeper"); err == nil {
			break
		}

		retries++
		log.Debug("WaitUntilConnected: retry=%d %v", retries, err)

		if d > 0 && time.Since(t1) > d {
			break
		} else if d > 0 {
			time.Sleep(d)
		} else {
			time.Sleep(c.sessionTimeout)
		}
	}

	log.Debug("zk Client WaitUntilConnected %s", time.Since(t1))
	return
}

func (c *Client) IsConnected() bool {
	return c != nil && c.isConnected.Get()
}

func (c *Client) SessionID() string {
	return strconv.FormatInt(c.zkConn.SessionID(), 10)
}

func (c *Client) Exists(path string) (result bool, err error) {
	if !c.IsConnected() {
		return false, ErrNotConnected
	}

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
	if !c.IsConnected() {
		return nil, nil, ErrNotConnected
	}

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
	if !c.IsConnected() {
		return ErrNotConnected
	}

	_, err := c.zkConn.Set(c.realPath(path), data, c.stat.Version)
	return err
}

func (c *Client) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	if !c.IsConnected() {
		return "", ErrNotConnected
	}

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
	if !c.IsConnected() {
		return ErrNotConnected
	}

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
