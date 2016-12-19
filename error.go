package zkclient

import (
	"fmt"
)

var (
	ErrNotConnected = fmt.Errorf("Not connected")
	ErrNotAllowed   = fmt.Errorf("Operation not allowed")
)

type ListenerError struct {
	Err  error
	Path string
}

func (le ListenerError) Error() string {
	return le.Err.Error()
}
