package zkclient

import (
	"fmt"
)

var (
	ErrNotConnected = fmt.Errorf("Not connected")
	ErrNotAllowed   = fmt.Errorf("Operation not allowed")
)

func wrapZkError(path string, err error) error {
	return fmt.Errorf("%s %v", path, err)
}
