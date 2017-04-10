package zkclient

import (
	"fmt"

	"github.com/funkygao/go-zookeeper/zk"
)

var (
	// ErrNotConnected means some operation must be executed after connection established.
	ErrNotConnected = fmt.Errorf("Not connected")

	// ErrNotAllowed means the operation is not allowed.
	ErrNotAllowed = fmt.Errorf("Operation not allowed")
)

// ListenerError wraps error from watchers.
type ListenerError struct {
	Err  error
	Path string
}

func (le ListenerError) Error() string {
	return le.Err.Error()
}

func IsErrNodeExists(err error) bool {
	if e, ok := err.(wrappedError); ok {
		return e.err == zk.ErrNodeExists
	}
	return err == zk.ErrNodeExists
}

func IsErrNoNode(err error) bool {
	if e, ok := err.(wrappedError); ok {
		return e.err == zk.ErrNoNode
	}
	return err == zk.ErrNoNode
}

func IsErrVersionConflict(err error) bool {
	if e, ok := err.(wrappedError); ok {
		return e.err == zk.ErrBadVersion
	}
	return err == zk.ErrBadVersion
}

type wrappedError struct {
	path string
	err  error
}

func (err wrappedError) Error() string {
	return fmt.Sprintf("%s %v", err.path, err.err)
}
