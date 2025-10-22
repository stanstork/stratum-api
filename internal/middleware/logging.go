package middleware

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/rs/zerolog"
)

type responseWriter struct {
	http.ResponseWriter
	status int
}

// Capture the status code before writing it.
func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

func LoggingMiddleware(logger zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			// Wrap the response writer to capture the status code
			rw := &responseWriter{ResponseWriter: w, status: http.StatusOK} // Default to 200

			rWithCtx := r.WithContext(logger.WithContext(r.Context()))

			// Call the next handler in the chain
			next.ServeHTTP(rw, rWithCtx)

			// Request is done. Log the event.
			duration := time.Since(start)

			logger.Info().
				Str("path", r.URL.Path).
				Int("status_code", rw.status).
				Dur("duration_ms", duration).
				Str("remote_addr", r.RemoteAddr).
				Str("user_agent", r.UserAgent()).
				Msg("HTTP")
		})
	}
}

// Retrieve the logger from the request context.
func GetLoggerFromContext(ctx context.Context) *zerolog.Logger {
	logger := zerolog.Ctx(ctx)
	if logger.GetLevel() == zerolog.Disabled {
		// Fallback to a default logger if none is in context
		log.Println("Warning: No logger found in context, falling back to default.")
		l := zerolog.New(os.Stdout).With().Timestamp().Logger()
		logger = &l
	}
	return logger
}
