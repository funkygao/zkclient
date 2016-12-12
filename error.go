package zkclient

import (
	"fmt"
)

var (
	ErrNotConnected = fmt.Errorf("Not connected")
)

func wrapZkError(path string, err error) error {
	return fmt.Errorf("%s %v", path, err)
}
