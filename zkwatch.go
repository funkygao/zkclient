package zkclient

import (
	"time"

	"github.com/funkygao/go-zookeeper/zk"
	log "github.com/funkygao/log4go"
)

const blindBackoff = time.Millisecond * 200

// SubscribeStateChanges MUST be called before Connect as we don't want
// to labor to handle the thread-safe issue.
func (c *Client) SubscribeStateChanges(listener ZkStateListener) {
	log.Debug("watching zookeeper session state changes")

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
			log.Debug("#%d got close signal, watchStateChanges quit", loops)
			return

		case evt = <-c.stateEvtCh:
			if evt.Path != "" {
				// not state change event, same logic as in Java org.I0Itec.zkclient
				// if Path not empty, it can be NodeDataChanged/NodeDeleted/NodeCreated/NodeChildrenChanged
				//
				// if evt.Type != zk.EventSession ?
				continue
			}

			log.Debug("#%d state event-> %+v", loops, evt)

			// TODO what if handler blocks?
			c.stateLock.Lock()
			for _, l := range c.stateChangeListeners {
				if err = l.HandleStateChanged(evt.State); err != nil {
					log.Error("HandleStateChanged#%d %v", loops, err)
				}
			}

			// extra handler for new session state
			if evt.State == zk.StateHasSession {
				c.isConnected.Set(true)
				for _, l := range c.stateChangeListeners {
					if err = l.HandleNewSession(); err != nil {
						log.Error("HandleNewSession#%d %v", loops, err)
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

	if !found {
		return
	}

	c.childChangeListeners[path] = newListeners
	if len(newListeners) == 0 {
		close(c.childWatchStopper[path]) // GC the watcher goroutine
		delete(c.childChangeListeners, path)
	}
}

func (c *Client) stopChildWatch(path string) {
	c.childLock.Lock()
	delete(c.childChangeListeners, path)
	c.childLock.Unlock()
}

func (c *Client) watchChildChanges(path string) {
	defer c.wg.Done()

	// TODO need this?
	if err := c.WaitUntilConnected(0); err != nil {
		log.Error("give up for %v", err)
		return
	}

	c.childLock.RLock()
	stopper := c.childWatchStopper[path]
	c.childLock.RUnlock()

	log.Debug("start watching child changes for %s", path)
	var (
		loops    int
		birthCry = false
	)
	for {
		loops++

		select {
		case <-c.close:
			log.Debug("%s#%d yes sir! watchChildChanges quit", path, loops)
			c.stopChildWatch(path)
			return
		case <-stopper:
			log.Debug("%s#%d yes sir! watchChildChanges stopped", path, loops)
			c.stopChildWatch(path)
			return
		default:
		}

		// because zk watcher is one-time trigger, if the event happends too fast
		// we might lose events between the watchers:
		// watches are used to find out about the latest change
		currentChilds, evtCh, err := c.ChildrenW(path)
		if err != nil {
			switch c.rawError(err) {
			case zk.ErrClosing:
				log.Debug("%s#%d zk closing", path, loops)
				c.stopChildWatch(path)
				return

			case zk.ErrNoNode:
				log.Debug("%s#%d %s, will retry after %s", path, loops, err, blindBackoff)
				time.Sleep(blindBackoff)
				continue

			default:
				log.Error("%s#%d %s", path, loops, err)
				c.fireListenerError(path, err)
				continue
			}
		}

		if c.birthCry && !birthCry {
			birthCry = true

			listeners := c.cloneChildChangeListeners()
			log.Debug("%s#%d birth cry to %d listeners", path, loops, len(listeners[path]))
			for _, l := range listeners[path] {
				if err = l.HandleChildChange(path, currentChilds); err != nil {
					log.Error("%s#%d %+v %v", path, loops, currentChilds, err)
				}
			}
		}

		log.Debug("%s#%d ok, waiting for child change event...", path, loops)
		select {
		case <-c.close:
			log.Debug("%s#%d yes sir! watchChildChanges quit", path, loops)
			c.stopChildWatch(path)
			return

		case <-stopper:
			log.Debug("%s#%d yes sir! watchChildChanges stopped", path, loops)
			c.stopChildWatch(path)
			return

		case evt, ok := <-evtCh:
			if !ok {
				log.Debug("%s#%d event channel closed, watchChildChanges quit", path, loops)
				c.stopChildWatch(path)
				return
			}

			if evt.Err == zk.ErrSessionExpired || evt.State == zk.StateDisconnected {
				// e,g.
				// {Type:EventNotWatching State:StateDisconnected Path:/foobar Err:zk: session has been expired by the server}
				log.Debug("%s#%d stop watching child for %+v", path, loops, evt)
				c.stopChildWatch(path)
				return
			}

			log.Debug("%s#%d child event-> %+v", path, loops, evt)

			if evt.Err != nil {
				log.Error("%s#%d unexpected event err %s", path, loops, evt.Err)
				c.fireListenerError(path, evt.Err)
				continue
			}

			if evt.Type != zk.EventNodeChildrenChanged {
				// ignore
				log.Debug("%s#%d ignored %+v", path, loops, evt)
				continue
			}

			listeners := c.cloneChildChangeListeners()
			log.Debug("%s#%d dispatching %+v to %d listeners", path, loops, currentChilds, len(listeners[path]))
			for _, l := range listeners[path] {
				if err = l.HandleChildChange(path, currentChilds); err != nil {
					log.Error("%s#%d %+v %v", path, loops, currentChilds, err)
				}
			}
		}
	}

}

func (c *Client) cloneChildChangeListeners() map[string][]ZkChildListener {
	c.childLock.Lock()
	listeners := make(map[string][]ZkChildListener, len(c.childChangeListeners))
	for k, v := range c.childChangeListeners {
		listeners[k] = v
	}
	c.childLock.Unlock()
	return listeners
}

func (c *Client) cloneDataChangeListeners() map[string][]ZkDataListener {
	c.dataLock.Lock()
	listeners := make(map[string][]ZkDataListener, len(c.dataChangeListeners))
	for k, v := range c.dataChangeListeners {
		listeners[k] = v
	}
	c.dataLock.Unlock()
	return listeners
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

func (c *Client) stopDataWatch(path string) {
	c.dataLock.Lock()
	delete(c.dataChangeListeners, path)
	c.dataLock.Unlock()
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

	log.Debug("start watching data changes for %s", path)
	var (
		loops    int
		birthCry = false
	)
	for {
		loops++

		select {
		case <-c.close:
			log.Debug("%s#%d yes sir! watchDataChanges quit", path, loops)
			c.stopDataWatch(path)
			return
		case <-stopper:
			log.Debug("%s#%d yes sir! watchDataChanges stopped", path, loops)
			c.stopDataWatch(path)
			return
		default:
		}

		data, evtCh, err := c.GetW(path)
		if err != nil {
			switch c.rawError(err) {
			case zk.ErrNoNode:
				log.Debug("%s#%d %s, will retry after %s", path, loops, err, blindBackoff)
				time.Sleep(blindBackoff)
				continue

			case zk.ErrClosing:
				log.Debug("%s#%d zk closing", path, loops)
				c.stopDataWatch(path)
				return

			default:
				log.Error("%s#%d %s", path, loops, err)
				c.fireListenerError(path, err)
				continue
			}
		}

		if c.birthCry && !birthCry {
			birthCry = true

			listeners := c.cloneDataChangeListeners()
			log.Debug("%s#%d birth cry to %d listeners", path, loops, len(listeners[path]))
			for _, l := range listeners[path] {
				if err = l.HandleDataChange(path, data); err != nil {
					log.Error("%s#%d %v", path, loops, err)
				}
			}
		}

		log.Debug("%s#%d ok, waiting for data change event...", path, loops)
		select {
		case <-c.close:
			log.Debug("%s#%d yes sir! watchDataChanges quit", path, loops)
			c.stopDataWatch(path)
			return

		case <-stopper:
			log.Debug("%s#%d yes sir! watchDataChanges stopped", path, loops)
			c.stopDataWatch(path)
			return

		case evt, ok := <-evtCh:
			if !ok {
				log.Debug("%s#%d event channel closed, watchDataChanges quit", path, loops)
				c.stopDataWatch(path)
				return
			}

			if evt.Err == zk.ErrSessionExpired || evt.State == zk.StateDisconnected {
				// e,g.
				// {Type:EventNotWatching State:StateDisconnected Path:/foobar Err:zk: session has been expired by the server}
				log.Debug("%s#%d stop watching data for %+v", path, loops, evt)
				c.stopDataWatch(path)
				return
			}

			log.Debug("%s#%d data event-> %+v", path, loops, evt)

			if evt.Err != nil {
				log.Error("%s#%d unexpected err %s", path, loops, evt.Err)
				c.fireListenerError(path, evt.Err)
				continue
			}

			if evt.Type != zk.EventNodeDataChanged && evt.Type != zk.EventNodeDeleted {
				// ignore
				log.Debug("%s#%d ignored %+v", path, loops, evt)
				continue
			}

			listeners := c.cloneDataChangeListeners()
			for _, l := range listeners[path] {
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

	if !found {
		return
	}

	c.dataChangeListeners[path] = newListeners
	if len(newListeners) == 0 {
		close(c.dataWatchStopper[path]) // GC the watcher goroutine
		delete(c.dataChangeListeners, path)
	}
}

func (c *Client) fireListenerError(path string, err error) {
	select {
	case c.lisenterErrCh <- ListenerError{Path: path, Err: err}:
	default:
		// discard silently
		log.Warn("%s silently ignored %v", path, err)
	}
}
