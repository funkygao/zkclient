package zkclient

type discardLogger struct{}

func (l discardLogger) Printf(format string, a ...interface{}) {}
