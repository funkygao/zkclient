package zkclient

import (
	"time"

	"github.com/yichen/retry"
)

var (
	zkRetryOptions = retry.RetryOptions{
		Tag:         "zkutil",
		Backoff:     time.Millisecond * 5,
		MaxBackoff:  time.Second * 1,
		Constant:    1,
		MaxAttempts: 1,
		UseV1Info:   false,
	}
)
