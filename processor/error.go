package processor

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type ErrCritical struct {
	Message string
	Inner   error
	Data    map[string]any
}

func (e *ErrCritical) Error() string {
	if e.Inner != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Inner)
	}
	return e.Message
}

func (e *ErrCritical) Unwrap() error {
	return e.Inner
}

func NewErrCritical(message string, inner error, data map[string]any) error {
	if inner == nil && message == "" {
		panic("ErrCritical requires either a message or an inner error")
	}
	return &ErrCritical{Message: message, Inner: inner, Data: data}
}

type ErrRetryable struct {
	Message string
	Inner   error
}

func (e *ErrRetryable) Error() string {
	if e.Inner != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Inner)
	}
	return e.Message
}

func (e *ErrRetryable) Unwrap() error {
	return e.Inner
}

func NewErrRetryable(message string, inner error) error {
	if inner == nil && message == "" {
		panic("ErrRetryable requires either a message or an inner error")
	}
	return &ErrRetryable{Message: message, Inner: inner}
}

type ErrIgnorable struct {
	Message string
	Inner   error
}

type ErrBlocking struct {
	Message string
	Inner   error
}

func (e *ErrIgnorable) Error() string {
	if e.Inner != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Inner)
	}
	return e.Message
}

func (e *ErrIgnorable) Unwrap() error {
	return e.Inner
}

func NewErrIgnorable(message string, inner error) error {
	if inner == nil && message == "" {
		panic("ErrIgnorable requires either a message or an inner error")
	}
	return &ErrIgnorable{Message: message, Inner: inner}
}

func (e *ErrBlocking) Error() string {
	if e.Inner != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Inner)
	}
	return e.Message
}

func (e *ErrBlocking) Unwrap() error {
	return e.Inner
}

func NewErrBlocking(message string, inner error) error {
	if inner == nil && message == "" {
		panic("ErrBlocking requires either a message or an inner error")
	}
	return &ErrBlocking{Message: message, Inner: inner}
}

const (
	maxRetryAttempts = 5
	retryDelay       = 5 * time.Second
)

// handleErr processes different types of errors and takes appropriate action
func (p *Processor) handleErr(err error, msg jetstream.Msg) {
	if err == nil {
		msg.Ack()
		return
	}

	metadata, errMetadata := msg.Metadata()
	if errMetadata != nil {
		slog.Error("Failed to retrieve message metadata",
			"error", errMetadata,
			"msgSubject", msg.Subject(),
		)
		// Continue processing with default metadata
		metadata = &jetstream.MsgMetadata{}
	}

	// Log common message context
	logCtx := []any{
		"msgId", msg.Subject(),
		"attempts", metadata.NumDelivered,
		"timestamp", metadata.Timestamp,
	}

	var errCritical *ErrCritical
	var errRetryable *ErrRetryable
	var errIgnorable *ErrIgnorable
	var errBlocking *ErrBlocking

	switch {
	case errors.As(err, &errCritical):
		handleCriticalError(errCritical, msg, logCtx)

	case errors.As(err, &errRetryable):
		handleRetryableError(errRetryable, msg, metadata.NumDelivered, logCtx)

	case errors.As(err, &errIgnorable):
		handleIgnorableError(errIgnorable, msg, logCtx)

	case errors.As(err, &errBlocking):
		handleBlockingError(errBlocking, msg, logCtx)

	default:
		// Treat unknown errors as critical
		slog.Error("Unknown error type encountered", append(logCtx,
			"error", err,
			"errorType", fmt.Sprintf("%T", err),
		)...)
		msg.Ack() // Acknowledge to prevent infinite retry
		os.Exit(-1)
	}
}

func handleCriticalError(err *ErrCritical, msg jetstream.Msg, logCtx []any) {
	slog.Error("Critical error encountered", append(logCtx,
		"errorMsg", err.Message,
		"innerError", err.Inner,
		"errorData", err.Data,
	)...)
	msg.Ack()
	os.Exit(-1)
}

func handleRetryableError(err *ErrRetryable, msg jetstream.Msg, attempts uint64, logCtx []any) {
	if attempts >= maxRetryAttempts {
		slog.Error("Max retry attempts reached", append(logCtx,
			"error", err,
			"maxAttempts", maxRetryAttempts,
		)...)
		msg.Ack()
		return
	}

	slog.Warn("Retryable error encountered", append(logCtx,
		"error", err,
		"nextRetryIn", retryDelay,
	)...)
	msg.NakWithDelay(retryDelay)
}

func handleIgnorableError(err *ErrIgnorable, msg jetstream.Msg, logCtx []any) {
	slog.Info("Ignorable error encountered", append(logCtx,
		"error", err,
	)...)
	msg.Ack()
}

func handleBlockingError(err *ErrBlocking, msg jetstream.Msg, logCtx []any) {
	slog.Info("Blocking error encountered - message will remain pending", append(logCtx,
		"subject", msg.Subject(),
		"error", err,
	)...)
	// Intentionally not acknowledging the message
	// This will cause the message to remain pending and be redelivered
	// after the consumer's AckWait timeout
}
