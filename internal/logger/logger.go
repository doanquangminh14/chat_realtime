package logger

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger wraps zap.Logger with additional context helpers
type Logger struct {
	*zap.Logger
}

// New creates a new structured logger
func New(level, format string) (*Logger, error) {
	zapLevel, err := zapcore.ParseLevel(level)
	if err != nil {
		return nil, fmt.Errorf("invalid log level %q: %w", level, err)
	}

	var encoderCfg zapcore.EncoderConfig
	if format == "json" {
		encoderCfg = zap.NewProductionEncoderConfig()
	} else {
		encoderCfg = zap.NewDevelopmentEncoderConfig()
		encoderCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}
	encoderCfg.TimeKey = "timestamp"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	var encoder zapcore.Encoder
	if format == "json" {
		encoder = zapcore.NewJSONEncoder(encoderCfg)
	} else {
		encoder = zapcore.NewConsoleEncoder(encoderCfg)
	}

	core := zapcore.NewCore(
		encoder,
		zapcore.AddSync(os.Stdout),
		zapLevel,
	)

	zapLogger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	return &Logger{zapLogger}, nil
}

// NewNop creates a no-op logger for testing
func NewNop() *Logger {
	return &Logger{zap.NewNop()}
}

// With returns a logger with additional fields
func (l *Logger) With(fields ...zap.Field) *Logger {
	return &Logger{l.Logger.With(fields...)}
}

// WithComponent returns a logger tagged with a component name
func (l *Logger) WithComponent(component string) *Logger {
	return l.With(zap.String("component", component))
}

// WithRequestID returns a logger tagged with a request ID
func (l *Logger) WithRequestID(requestID string) *Logger {
	return l.With(zap.String("request_id", requestID))
}

// Sync flushes any buffered log entries
func (l *Logger) Sync() {
	_ = l.Logger.Sync()
}
