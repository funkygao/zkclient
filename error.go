package zkclient

import (
	"fmt"
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
