package srad

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/CVDpl/go-live-srad/internal/common"
)

// DefaultLogger implements the Logger interface with structured JSON logging.
type DefaultLogger struct {
	mu     sync.Mutex
	level  common.LogLevel
	logger *log.Logger
	fields map[string]interface{}
}

// NewDefaultLogger creates a new default logger.
func NewDefaultLogger() common.Logger {
	return &DefaultLogger{
		level:  common.LogLevelInfo,
		logger: log.New(os.Stderr, "", 0),
		fields: make(map[string]interface{}),
	}
}

// NewDefaultLoggerWithLevel creates a logger with a specific log level.
func NewDefaultLoggerWithLevel(level common.LogLevel) common.Logger {
	return &DefaultLogger{
		level:  level,
		logger: log.New(os.Stderr, "", 0),
		fields: make(map[string]interface{}),
	}
}

// Debug logs a debug message.
func (l *DefaultLogger) Debug(msg string, fields ...interface{}) {
	if l.level <= common.LogLevelDebug {
		l.log("DEBUG", msg, fields...)
	}
}

// Info logs an info message.
func (l *DefaultLogger) Info(msg string, fields ...interface{}) {
	if l.level <= common.LogLevelInfo {
		l.log("INFO", msg, fields...)
	}
}

// Warn logs a warning message.
func (l *DefaultLogger) Warn(msg string, fields ...interface{}) {
	if l.level <= common.LogLevelWarn {
		l.log("WARN", msg, fields...)
	}
}

// Error logs an error message.
func (l *DefaultLogger) Error(msg string, fields ...interface{}) {
	if l.level <= common.LogLevelError {
		l.log("ERROR", msg, fields...)
	}
}

// log formats and outputs a log message.
func (l *DefaultLogger) log(level, msg string, fields ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry := map[string]interface{}{
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"level":     level,
		"message":   msg,
	}

	// Process fields as key-value pairs
	for i := 0; i < len(fields)-1; i += 2 {
		if key, ok := fields[i].(string); ok {
			entry[key] = fields[i+1]
		}
	}

	// Add persistent fields
	for k, v := range l.fields {
		if _, exists := entry[k]; !exists {
			entry[k] = v
		}
	}

	data, err := json.Marshal(entry)
	if err != nil {
		l.logger.Printf(`{"level":"ERROR","message":"failed to marshal log entry","error":"%s"}`, err)
		return
	}

	l.logger.Println(string(data))
}

// WithFields returns a logger with additional persistent fields.
func (l *DefaultLogger) WithFields(fields map[string]interface{}) common.Logger {
	newLogger := &DefaultLogger{
		level:  l.level,
		logger: l.logger,
		fields: make(map[string]interface{}),
	}

	// Copy existing fields
	for k, v := range l.fields {
		newLogger.fields[k] = v
	}

	// Add new fields
	for k, v := range fields {
		newLogger.fields[k] = v
	}

	return newLogger
}

// NullLogger is a logger that discards all log messages.
type NullLogger struct{}

// NewNullLogger creates a logger that discards all messages.
func NewNullLogger() common.Logger {
	return &NullLogger{}
}

func (n *NullLogger) Debug(msg string, fields ...interface{}) {}
func (n *NullLogger) Info(msg string, fields ...interface{})  {}
func (n *NullLogger) Warn(msg string, fields ...interface{})  {}
func (n *NullLogger) Error(msg string, fields ...interface{}) {}

// LoggerWithContext wraps a logger with contextual information.
type LoggerWithContext struct {
	logger common.Logger
	fields map[string]interface{}
}

// WithContext adds contextual fields to a logger.
func WithContext(logger common.Logger, fields map[string]interface{}) common.Logger {
	if logger == nil {
		logger = NewDefaultLogger()
	}

	// If it's already a LoggerWithContext, merge fields
	if lwc, ok := logger.(*LoggerWithContext); ok {
		newFields := make(map[string]interface{})
		for k, v := range lwc.fields {
			newFields[k] = v
		}
		for k, v := range fields {
			newFields[k] = v
		}
		return &LoggerWithContext{
			logger: lwc.logger,
			fields: newFields,
		}
	}

	return &LoggerWithContext{
		logger: logger,
		fields: fields,
	}
}

func (l *LoggerWithContext) Debug(msg string, fields ...interface{}) {
	l.logger.Debug(msg, l.mergeFields(fields...)...)
}

func (l *LoggerWithContext) Info(msg string, fields ...interface{}) {
	l.logger.Info(msg, l.mergeFields(fields...)...)
}

func (l *LoggerWithContext) Warn(msg string, fields ...interface{}) {
	l.logger.Warn(msg, l.mergeFields(fields...)...)
}

func (l *LoggerWithContext) Error(msg string, fields ...interface{}) {
	l.logger.Error(msg, l.mergeFields(fields...)...)
}

func (l *LoggerWithContext) mergeFields(fields ...interface{}) []interface{} {
	result := make([]interface{}, 0, len(fields)+len(l.fields)*2)

	// Add contextual fields
	for k, v := range l.fields {
		result = append(result, k, v)
	}

	// Add provided fields
	result = append(result, fields...)

	return result
}

// LogError is a helper to log an error with context.
func LogError(logger common.Logger, msg string, err error, fields ...interface{}) {
	allFields := append([]interface{}{"error", err.Error()}, fields...)
	logger.Error(msg, allFields...)
}

// LogLatency is a helper to log operation latency.
func LogLatency(logger common.Logger, operation string, start time.Time, fields ...interface{}) {
	duration := time.Since(start)
	allFields := append([]interface{}{
		"operation", operation,
		"duration_ms", duration.Milliseconds(),
		"duration_ns", duration.Nanoseconds(),
	}, fields...)

	if duration > time.Second {
		logger.Warn(fmt.Sprintf("slow operation: %s", operation), allFields...)
	} else {
		logger.Debug(fmt.Sprintf("operation completed: %s", operation), allFields...)
	}
}
