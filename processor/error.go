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

func (e *ErrIgnorable) Error() string {
	if e.Inner != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Inner)
	}
	return e.Message
}

func NewErrIgnorable(message string, inner error) error {
	if inner == nil && message == "" {
		panic("ErrIgnorable requires either a message or an inner error")
	}
	return &ErrIgnorable{Message: message, Inner: inner}
}

func (p *Processor) handleErr(err error, msg jetstream.Msg) {
	var errCritical *ErrCritical
	var errRetryable *ErrRetryable
	var errIgnorable *ErrIgnorable

	if errors.As(err, &errCritical) {
		// Handle critical error
		slog.Error("Critical error encountered",
			"errorMsg", errCritical.Message,
			"innerError", errCritical.Inner,
			"errorData", errCritical.Data,
		)
		msg.Ack()
		os.Exit(-1)
	} else if errors.As(err, &errRetryable) {
		// Handle retryable error
		metadata, errMetadata := msg.Metadata()
		if errMetadata != nil {
			slog.Error("error retrieving msg metadata", "error", errMetadata)
		}
		attempts := metadata.NumDelivered
		if attempts >= 5 {
			slog.Error("Max retry attempts reached, handling message", "error", errRetryable, "attempts", attempts)
			msg.Ack()
		} else {
			slog.Error("Retryable error encountered, will attempt to reprocess", "error", errRetryable, "attempts", attempts)
			msg.NakWithDelay(time.Duration(5) * time.Second)
			os.Exit(-1)
		}
	} else if errors.As(err, &errIgnorable) {
		// Handle ignorable error
		slog.Info("Ignorable error encountered, proceeding", "error", errIgnorable)
		msg.Ack()
	} else {
		// Handle unknown error
		slog.Error("Unknown error type", "error", err)
		msg.Ack()
	}

}
