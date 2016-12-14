package zkclient

import (
	"fmt"
)

var (
	ErrNotConnected = fmt.Errorf("Not connected")
	ErrNotAllowed   = fmt.Errorf("Operation not allowed")
)
