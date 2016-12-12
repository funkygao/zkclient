package zkclient

import (
	"time"

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
