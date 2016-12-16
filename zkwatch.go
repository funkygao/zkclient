package zkclient

import (
	"time"

	"github.com/funkygao/go-zookeeper/zk"
	log "github.com/funkygao/log4go"
)

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

func (c *Client) fireListenerError(err error) {
	select {
	case c.lisenterErrCh <- err:
	default:
		// discard silently
	}
}
