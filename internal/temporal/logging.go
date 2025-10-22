package temporal

import (
	"github.com/rs/zerolog"
	"go.temporal.io/sdk/log"
)

type TemporalAdapter struct {
	logger zerolog.Logger
}

func NewTemporalAdapter(logger zerolog.Logger) log.Logger {
	return &TemporalAdapter{
		logger: logger.With().Str("component", "temporal-sdk").Logger(),
	}
}

func (a *TemporalAdapter) withKeyvals(event *zerolog.Event, keyvals ...interface{}) *zerolog.Event {
	if len(keyvals) == 0 {
		return event
	}
	// Ensure even number of keyvals. If not, add a placeholder.
	if len(keyvals)%2 != 0 {
		keyvals = append(keyvals, "MISSING_VALUE")
	}

	for i := 0; i < len(keyvals); i += 2 {
		key, ok := keyvals[i].(string)
		if !ok {
			// Handle non-string keys gracefully, though they are not expected
			key = "INVALID_KEY"
		}
		event = event.Interface(key, keyvals[i+1])
	}
	return event
}

// Debug logs a debug message.
func (a *TemporalAdapter) Debug(msg string, keyvals ...interface{}) {
	a.withKeyvals(a.logger.Debug(), keyvals...).Msg(msg)
}

// Info logs an info message.
func (a *TemporalAdapter) Info(msg string, keyvals ...interface{}) {
	a.withKeyvals(a.logger.Info(), keyvals...).Msg(msg)
}

// Warn logs a warning message.
func (a *TemporalAdapter) Warn(msg string, keyvals ...interface{}) {
	a.withKeyvals(a.logger.Warn(), keyvals...).Msg(msg)
}

// Error logs an error message.
func (a *TemporalAdapter) Error(msg string, keyvals ...interface{}) {
	a.withKeyvals(a.logger.Error(), keyvals...).Msg(msg)
}
