package common

// NullLogger discards all log messages.
type NullLogger struct{}

// NewNullLogger creates a logger that discards all messages.
func NewNullLogger() Logger { return &NullLogger{} }

func (n *NullLogger) Debug(msg string, fields ...interface{}) {}
func (n *NullLogger) Info(msg string, fields ...interface{})  {}
func (n *NullLogger) Warn(msg string, fields ...interface{})  {}
func (n *NullLogger) Error(msg string, fields ...interface{}) {}
