package zkclient

import (
	gopath "path"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/funkygao/go-zookeeper/zk"
	"github.com/funkygao/golib/debug"
	"github.com/funkygao/golib/sync2"
	log "github.com/funkygao/log4go"
	"github.com/funkygao/zkclient/retry"
)

// Client is the zookeeper connection wrapped on zk.Conn with the following features:
// chroot, subscribe/unsubscribe, retry, ACL, create/delete recursively.
type Client struct {
	zkSvr, chroot     string
	servers           []string
	sessionTimeout    time.Duration
	withRetry         bool
	wrapErrorWithPath bool

	connectOnce   sync.Once
	isConnected   sync2.AtomicBool
	connectCalled chan struct{}
	close         chan struct{}
	wg            sync.WaitGroup

	zkConn     *zk.Conn
	logger     zk.Logger
	stat       *zk.Stat // storage for the lastest zk query stat info FIXME
	stateEvtCh <-chan zk.Event
	acl        []zk.ACL

	lisenterErrCh chan ListenerError

	stateLock            sync.RWMutex
	stateChangeListeners []ZkStateListener

	birthCry bool

	childLock            sync.RWMutex
	childChangeListeners map[string][]ZkChildListener
	childWatchStopper    map[string]chan struct{}

	dataLock            sync.RWMutex
	dataChangeListeners map[string][]ZkDataListener
	dataWatchStopper    map[string]chan struct{}

	// Debug will turn on/off the debug mode.
	Debug bool
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
		birthCry:             false,
		childChangeListeners: map[string][]ZkChildListener{},
		childWatchStopper:    map[string]chan struct{}{},
		dataChangeListeners:  map[string][]ZkDataListener{},
		dataWatchStopper:     map[string]chan struct{}{},
		Debug:                false,
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

	c.connectCalled = make(chan struct{})
	zkConn, stateEvtCh, err := zk.Connect(c.servers, c.sessionTimeout)
	if err != nil {
		return err
	}

	if c.logger != nil {
		zkConn.SetLogger(c.logger)
	}

	close(c.connectCalled)
	c.close = make(chan struct{})
	c.lisenterErrCh = make(chan ListenerError, 10)
	c.zkConn = zkConn
	c.stateEvtCh = stateEvtCh

	if c.chroot != "" {
		if err := c.ensurePathExists(c.chroot); err != nil {
			return err
		}
	}

	// always watch state changes to maintain IsConnected
	c.connectOnce.Do(func() {
		c.wg.Add(1)
		go c.watchStateChanges()
	})

	log.Debug("Connected in %s", time.Since(t1))

	return nil
}

// Disconnect will disconnect from the zookeeper ensemble and release related resources.
func (c *Client) Disconnect() {
	log.Debug("Disconnecting...")

	t1 := time.Now()

	close(c.close)
	c.wg.Wait()

	if c.zkConn != nil {
		c.zkConn.Close()
	}

	c.zkConn = nil
	c.stat = nil
	c.isConnected.Set(false)
	close(c.lisenterErrCh)

	log.Debug("Disconnected in %s", time.Since(t1))
}

// ZkSvr returns the raw zookeeper servers connection string.
func (c *Client) ZkSvr() string {
	return c.zkSvr
}

// Auth will let zk client do auth to zk server with digest and raw user:passwd.
func (c *Client) Auth(userColonPasswd string) error {
	return c.zkConn.AddAuth("digest", []byte(userColonPasswd))
}

func (c *Client) DiscardZkLogger() {
	c.logger = discardLogger{}
	if c.zkConn != nil {
		c.zkConn.SetLogger(c.logger)
	}
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

func (c *Client) SetLisenterBirthCry(yes bool) {
	c.birthCry = yes
}

func (c *Client) realPath(path string) string {
	if c.chroot == "" {
		return path
	}

	return gopath.Join(c.chroot, path)
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

	if c.Debug {
		log.Debug("WaitUntilConnected %s %+v", time.Since(t1), debug.Callstack(2))
	}
	return
}

func (c *Client) IsConnected() bool {
	return c != nil && c.isConnected.Get()
}

func (c *Client) SessionID() string {
	return strconv.FormatInt(c.zkConn.SessionID(), 10)
}

// LisenterErrors returns a channel that you can read to obtain errors from all
// but state change listeners.
func (c *Client) LisenterErrors() <-chan ListenerError {
	return c.lisenterErrCh
}

// WalkFunc is the type of the function called for each znode visited by Walk.
type WalkFunc func(path string, stat *zk.Stat, err error) error

// Walk walks the znode tree rooted at root, calling walkFn for each znode
// in the tree, including root.
// All errors that arise visiting znode are filtered by walkFn.
// The znodes are walked in lexical order, which makes the output deterministic
// but means that for very large znode tree Walk can be inefficient.
func (c *Client) Walk(root string, walkFn WalkFunc) error {
	exists, stat, err := c.zkConn.Exists(c.realPath(root))
	if err != nil {
		return walkFn(root, nil, err)
	}
	if !exists {
		return walkFn(root, nil, zk.ErrNoNode)
	}

	return c.walk(root, stat, walkFn)
}

func (c *Client) walk(path string, stat *zk.Stat, walkFn WalkFunc) error {
	if err := walkFn(path, stat, nil); err != nil {
		return err
	}

	children, err := c.Children(c.realPath(path))
	if err != nil {
		return walkFn(path, stat, err)
	}

	sort.Strings(children)
	for _, child := range children {
		childPath := gopath.Join(path, child)
		_, childStat, err := c.zkConn.Exists(c.realPath(childPath))
		if err != nil {
			if e := walkFn(childPath, childStat, err); e != nil {
				return e
			}
		} else if e := c.walk(childPath, childStat, walkFn); e != nil {
			return e
		}

	}

	return nil
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
		err = c.wrapZkError(path, err)
	}

	return
}

func (c *Client) ExistsAll(paths ...string) (bool, error) {
	for _, path := range paths {
		if exists, err := c.Exists(path); err != nil || exists == false {
			return exists, c.wrapZkError(path, err)
		}
	}

	return true, nil
}

func (c *Client) GetWithStat(path string) (data []byte, stat *zk.Stat, err error) {
	if c.withRetry {
		err = retry.RetryWithBackoff(zkRetryOptions, func() (retry.RetryStatus, error) {
			data, stat, err = c.zkConn.Get(c.realPath(path))
			if err != nil {
				return retry.RetryContinue, c.wrapZkError(path, err)
			}

			return retry.RetryBreak, nil
		})
	} else {
		data, stat, err = c.zkConn.Get(c.realPath(path))
		err = c.wrapZkError(path, err)
	}

	return
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
		err = c.wrapZkError(path, err)
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
		err = c.wrapZkError(path, err)
	}

	return
}

// Set will set the znode path with data without regarding version.
// If you care about atomic Set(CAS), use SetWithVersion.
func (c *Client) Set(path string, data []byte) error {
	_, err := c.zkConn.Set(c.realPath(path), data, -1)
	err = c.wrapZkError(path, err)
	return err
}

// SetWithVersion is CAS version of Set.
func (c *Client) SetWithVersion(path string, data []byte, version int32) (*zk.Stat, error) {
	stat, err := c.zkConn.Set(c.realPath(path), data, version)
	err = c.wrapZkError(path, err)
	return stat, err
}

func (c *Client) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	p, err := c.zkConn.Create(path, data, flags, acl)
	err = c.wrapZkError(path, err)
	return p, err
}

func (c *Client) CreatePersistentIfNotPresent(path string, data []byte) error {
	if err := c.CreatePersistent(path, data); !c.isZkError(err, zk.ErrNodeExists) {
		return err
	}

	return nil
}

func (c *Client) CreateEmptyPersistentIfNotPresent(path string) error {
	return c.CreatePersistentIfNotPresent(path, []byte{})
}

func (c *Client) CreatePersistent(path string, data []byte) error {
	if err := c.ensurePathExists(c.realPath(gopath.Dir(path))); err != nil {
		return c.wrapZkError(path, err)
	}

	flags := int32(0)
	_, err := c.Create(path, data, flags, c.acl)
	return c.wrapZkError(path, err)
}

func (c *Client) CreateEmptyPersistent(path string) error {
	return c.CreatePersistent(path, []byte{})
}

func (c *Client) CreateEphemeral(path string, data []byte) error {
	if err := c.ensurePathExists(c.realPath(gopath.Dir(path))); err != nil {
		return c.wrapZkError(path, err)
	}

	flags := int32(zk.FlagEphemeral)
	_, err := c.Create(path, data, flags, c.acl)
	return c.wrapZkError(path, err)
}

func (c *Client) CreatePersistentRecord(path string, r Marshaller) error {
	if err := c.ensurePathExists(c.realPath(gopath.Dir(path))); err != nil {
		return c.wrapZkError(path, err)
	}

	return c.CreatePersistent(path, r.Marshal())
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
		err = c.wrapZkError(path, err)
	}

	return
}

func (c *Client) ChildrenValues(p string) (children []string, values [][]byte, err error) {
	if c.withRetry {
		err = retry.RetryWithBackoff(zkRetryOptions, func() (retry.RetryStatus, error) {
			children, c.stat, err = c.zkConn.Children(c.realPath(p))
			if err != nil {
				return retry.RetryContinue, c.wrapZkError(p, err)
			}

			sort.Strings(children)

			values = make([][]byte, 0, len(children))
			var data []byte
			for _, child := range children {
				if data, err = c.Get(gopath.Join(p, child)); err != nil {
					return retry.RetryContinue, c.wrapZkError(p, err)
				}

				values = append(values, data)
			}

			return retry.RetryBreak, nil
		})
	} else {
	RETRY:
		children, c.stat, err = c.zkConn.Children(c.realPath(p))
		if err != nil {
			err = c.wrapZkError(p, err)
			return
		}

		sort.Strings(children)

		values = make([][]byte, 0, len(children))
		var data []byte
		for _, child := range children {
			if data, err = c.Get(gopath.Join(p, child)); err != nil {
				if err == zk.ErrNoNode {
					// between Children() and Get() the znode might change: just get the latest znode
					time.Sleep(blindBackoff)
					goto RETRY
				}

				// unexpeced err
				return
			}

			values = append(values, data)
		}
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
		err = c.wrapZkError(path, err)
	}

	return
}

func (c *Client) Delete(path string) error {
	err := c.zkConn.Delete(c.realPath(path), -1)
	return c.wrapZkError(path, err)
}

func (c *Client) DeleteTree(path string) error {
	return c.deleteTreeRealPath(c.realPath(path))
}

func (c *Client) deleteTreeRealPath(path string) error {
	if exists, _, err := c.zkConn.Exists(path); !exists || err != nil {
		return c.wrapZkError(path, err)
	}

	children, _, err := c.zkConn.Children(path)
	if err != nil {
		return c.wrapZkError(path, err)
	}

	if len(children) == 0 {
		err := c.zkConn.Delete(path, -1)
		return c.wrapZkError(path, err)
	}

	for _, child := range children {
		p := path + "/" + child
		e := c.deleteTreeRealPath(p)
		if e != nil {
			return c.wrapZkError(path, e)
		}
	}

	return c.zkConn.Delete(path, -1)
}

func (c *Client) ensurePathExists(p string) error {
	if exists, _, _ := c.zkConn.Exists(p); exists {
		return nil
	}

	parent := gopath.Dir(p)
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
	if err == nil || !c.wrapErrorWithPath {
		return err
	}

	if _, wrapped := err.(wrappedError); wrapped {
		return err
	}

	return wrappedError{path: path, err: err}
}

func (c *Client) isZkError(err, zkErr error) bool {
	if err == nil || zkErr == nil {
		return false
	}

	if e, ok := err.(wrappedError); ok {
		return e.err == zkErr
	}

	return err == zkErr
}

func (c *Client) rawError(err error) error {
	if err == nil || !c.wrapErrorWithPath {
		return err
	}

	if e, ok := err.(wrappedError); ok {
		return e.err
	}

	return err
}
