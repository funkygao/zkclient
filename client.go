package zkclient

import (
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-zookeeper/zk"
	"github.com/funkygao/golib/sync2"
	log "github.com/funkygao/log4go"
	"github.com/yichen/retry"
)

var (
	zkRetryOptions = retry.RetryOptions{
		"zookeeper",          // tag
		time.Millisecond * 5, // backoff
		time.Second * 1,      // max backoff
		1,                    // default backoff constant
		1,                    // MaxAttempts, 0 means infinite
		false,                // use V(1) level for log messages
	}
)

type client struct {
	sync.RWMutex

	zkSvr, chroot  string
	servers        []string
	sessionTimeout time.Duration

	isConnected sync2.AtomicBool
	close       chan struct{}
	wg          sync.WaitGroup

	zkConn     *zk.Conn
	stat       *zk.Stat // storage for the lastest zk query stat info FIXME
	stateEvtCh <-chan zk.Event
	acl        []zk.ACL

	stateChangeListeners []ZkStateListener
}

func New(zkSvr string, options ...clientOption) *client {
	servers, chroot, err := parseZkConnStr(zkSvr)
	if err != nil || len(servers) == 0 {
		// yes, panic!
		panic("invalid zkSvr")
	}

	conn := &client{
		zkSvr:                zkSvr,
		chroot:               chroot,
		servers:              servers,
		close:                make(chan struct{}),
		sessionTimeout:       time.Second * 30,
		acl:                  zk.WorldACL(zk.PermAll),
		stateChangeListeners: []ZkStateListener{},
	}
	conn.isConnected.Set(false)

	for _, option := range options {
		option(conn)
	}

	return conn
}

func (conn *client) Connect() error {
	t1 := time.Now()
	zkConn, stateEvtCh, err := zk.Connect(conn.servers, conn.sessionTimeout)
	if err != nil {
		return err
	}

	conn.zkConn = zkConn
	conn.stateEvtCh = stateEvtCh

	if conn.chroot != "" {
		if err := conn.ensurePathExists(conn.chroot); err != nil {
			return err
		}
	}

	conn.wg.Add(1)
	go conn.watchStateChanges()

	log.Debug("zk client Connect %s", time.Since(t1))

	return nil
}

func (conn *client) Disconnect() {
	t1 := time.Now()
	if conn.zkConn != nil {
		conn.zkConn.Close()
	}
	close(conn.close)
	conn.wg.Wait()
	conn.isConnected.Set(false)

	log.Debug("zk client Disconnect %s", time.Since(t1))
}

// SubscribeStateChanges MUST be called before Connect as we don't want
// to labor to handle the thread-safe issue.
func (conn *client) SubscribeStateChanges(l ZkStateListener) {
	conn.stateChangeListeners = append(conn.stateChangeListeners, l)
}

func (conn *client) watchStateChanges() {
	defer conn.wg.Done()

	var evt zk.Event
	for {
		select {
		case <-conn.close:
			log.Debug("zk client got close signal, stopped ok")
			return

		case evt = <-conn.stateEvtCh:
			// TODO lock? currently, SubscribeStateChanges must called before Connect
			// what if handler blocks?
			for _, l := range conn.stateChangeListeners {
				l.HandleStateChanged(evt.State)
			}

			// extra handler for new session state
			if evt.State == zk.StateHasSession {
				conn.isConnected.Set(true)
				for _, l := range conn.stateChangeListeners {
					l.HandleNewSession()
				}
			} else if evt.State != zk.StateUnknown {
				conn.isConnected.Set(false)
			}
		}
	}
}

func (conn client) realPath(path string) string {
	if conn.chroot == "" {
		return path
	}

	return strings.TrimRight(conn.chroot+path, "/")
}

func (conn *client) WaitUntilConnected(d time.Duration) (err error) {
	t1 := time.Now()
	retries := 0
	for {
		if _, _, err = conn.zkConn.Exists("/zookeeper"); err == nil {
			break
		}

		retries++
		log.Debug("WaitUntilConnected: retry=%d %v", retries, err)

		if d > 0 && time.Since(t1) > d {
			break
		} else if d > 0 {
			time.Sleep(d)
		} else {
			time.Sleep(conn.sessionTimeout)
		}
	}

	log.Debug("zk client WaitUntilConnected %s", time.Since(t1))
	return
}

func (conn *client) IsConnected() bool {
	return conn != nil && conn.isConnected.Get()
}

func (conn *client) SessionID() string {
	return strconv.FormatInt(conn.zkConn.SessionID(), 10)
}

func (conn *client) CreatePersistentRecord(p string, r Record) error {
	parent := path.Dir(p)
	err := conn.ensurePathExists(conn.realPath(parent))
	if err != nil {
		return err
	}

	return conn.CreatePersistent(p, r.Marshal())
}

func (conn *client) SetRecord(path string, r Record) error {
	exists, err := conn.Exists(path)
	if err != nil {
		return err
	}

	if !exists {
		if err = conn.ensurePathExists(conn.realPath(path)); err != nil {
			return err
		}
	}

	if _, err = conn.Get(path); err != nil {
		return err
	}

	return conn.Set(path, r.Marshal())
}

func (conn *client) Exists(path string) (bool, error) {
	if !conn.IsConnected() {
		return false, helix.ErrNotConnected
	}

	var result bool
	var stat *zk.Stat

	err := retry.RetryWithBackoff(zkRetryOptions, func() (retry.RetryStatus, error) {
		r, s, err := conn.zkConn.Exists(conn.realPath(path))
		if err != nil {
			return retry.RetryContinue, conn.wrapZkError(conn.realPath(path), err)
		}

		// ok
		result = r
		stat = s
		return retry.RetryBreak, nil
	})

	conn.stat = stat
	return result, err
}

func (conn *client) ExistsAll(paths ...string) (bool, error) {
	for _, path := range paths {
		if exists, err := conn.Exists(path); err != nil || exists == false {
			return exists, err
		}
	}

	return true, nil
}

func (conn *client) Get(path string) ([]byte, error) {
	if !conn.IsConnected() {
		return nil, ErrNotConnected
	}

	var data []byte

	err := retry.RetryWithBackoff(zkRetryOptions, func() (retry.RetryStatus, error) {
		d, s, err := conn.zkConn.Get(conn.realPath(path))
		if err != nil {
			return retry.RetryContinue, conn.wrapZkError(conn.realPath(path), err)
		}

		// ok
		data = d
		conn.stat = s
		return retry.RetryBreak, nil
	})

	return data, err
}

func (conn *client) GetW(path string) ([]byte, <-chan zk.Event, error) {
	if !conn.IsConnected() {
		return nil, nil, ErrNotConnected
	}

	var data []byte
	var events <-chan zk.Event

	err := retry.RetryWithBackoff(zkRetryOptions, func() (retry.RetryStatus, error) {
		d, s, evts, err := conn.zkConn.GetW(conn.realPath(path))
		if err != nil {
			return retry.RetryContinue, conn.wrapZkError(conn.realPath(path), err)
		}

		// ok
		data = d
		conn.stat = s
		events = evts
		return retry.RetryBreak, nil
	})

	return data, events, err
}

func (conn *client) Set(path string, data []byte) error {
	if !conn.IsConnected() {
		return ErrNotConnected
	}

	_, err := conn.zkConn.Set(conn.realPath(path), data, conn.stat.Version)
	return err
}

func (conn *client) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	if !conn.IsConnected() {
		return "", ErrNotConnected
	}

	return conn.zkConn.Create(conn.realPath(path), data, flags, acl)
}

func (conn *client) CreatePersistent(path string, data []byte) error {
	flags := int32(0)
	_, err := conn.Create(path, data, flags, conn.acl)
	return err
}

func (conn *client) CreateEphemeral(path string, data []byte) error {
	flags := int32(zk.FlagEphemeral)
	_, err := conn.Create(path, data, flags, conn.acl)
	return err
}

func (conn *client) CreateEmptyPersistent(path string) error {
	return conn.CreatePersistent(path, []byte{})
}

func (conn *client) Children(path string) ([]string, error) {
	if !conn.IsConnected() {
		return nil, ErrNotConnected
	}

	var children []string
	err := retry.RetryWithBackoff(zkRetryOptions, func() (retry.RetryStatus, error) {
		c, s, err := conn.zkConn.Children(conn.realPath(path))
		if err != nil {
			return retry.RetryContinue, conn.wrapZkError(conn.realPath(path), err)
		}

		// ok
		children = c
		conn.stat = s
		return retry.RetryBreak, nil
	})

	return children, err
}

func (conn *client) ChildrenW(path string) ([]string, <-chan zk.Event, error) {
	if !conn.IsConnected() {
		return nil, nil, ErrNotConnected
	}

	var children []string
	var eventChan <-chan zk.Event

	err := retry.RetryWithBackoff(zkRetryOptions, func() (retry.RetryStatus, error) {
		c, s, evts, err := conn.zkConn.ChildrenW(conn.realPath(path))
		if err != nil {
			return retry.RetryContinue, conn.wrapZkError(conn.realPath(path), err)
		}
		children = c
		conn.stat = s
		eventChan = evts
		return retry.RetryBreak, nil
	})

	return children, eventChan, err
}

func (conn *client) Delete(path string) error {
	return conn.zkConn.Delete(conn.realPath(path), -1)
}

func (conn *client) DeleteTree(path string) error {
	if !conn.IsConnected() {
		return ErrNotConnected
	}

	return conn.deleteTreeRealPath(conn.realPath(path))
}

func (conn *client) deleteTreeRealPath(path string) error {
	if exists, _, err := conn.zkConn.Exists(path); !exists || err != nil {
		return err
	}

	children, _, err := conn.zkConn.Children(path)
	if err != nil {
		return err
	}

	if len(children) == 0 {
		err := conn.zkConn.Delete(path, -1)
		return err
	}

	for _, c := range children {
		p := path + "/" + c
		e := conn.deleteTreeRealPath(p)
		if e != nil {
			return e
		}
	}

	return conn.zkConn.Delete(path, -1)
}

func (conn *client) ensurePathExists(p string) error {
	if exists, _, _ := conn.zkConn.Exists(p); exists {
		return nil
	}

	parent := path.Dir(p)
	if exists, _, _ := conn.zkConn.Exists(parent); !exists {
		if err := conn.ensurePathExists(parent); err != nil {
			return err
		}
	}

	flags := int32(0)
	conn.zkConn.Create(p, []byte{}, flags, conn.acl)
	return nil
}

func (conn *client) wrapZkError(path string, err error) error {
	return fmt.Errorf("%s %v", path, err)
}
