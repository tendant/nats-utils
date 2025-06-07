package processor

import (
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const defaultFetchTimeout = 60 * time.Second

// ConditionResult represents the result of a condition check
type ConditionResult struct {
	// ShouldProcess indicates whether the message should be processed
	ShouldProcess bool

	// RetryDelay specifies how long to wait before retrying if ShouldProcess is false
	// A zero value means no retry
	RetryDelay time.Duration

	// Reason provides context about why the condition check passed or failed
	Reason string

	// MaxRetries specifies a custom max retry count for this specific message
	// If set to 0, the system default is used
	MaxRetries int

	// Metadata contains additional context-specific information
	// that might be useful for logging or debugging
	Metadata map[string]interface{}

	// TerminateIfFailed indicates whether to terminate processing (ack the message)
	// if the condition is not met, rather than scheduling a retry
	TerminateIfFailed bool
}

// ConditionCheckFn is a function that checks if conditions are met for processing a message
type ConditionCheckFn func(jetstream.Msg) (ConditionResult, error)

// Processor represents the state and configuration of a NATS processor.
type Processor struct {
	consumer     jetstream.Consumer
	processFn    ProcessFn
	fetchTimeout time.Duration
	conditionFn  ConditionCheckFn
}

// WithFetchTimeout sets the timeout duration for fetching messages.
// If not set, defaults to 60 seconds.
func WithFetchTimeout(timeout time.Duration) func(*Processor) {
	return func(p *Processor) {
		p.fetchTimeout = timeout
	}
}

func WithConditionCheck(conditionFn ConditionCheckFn) func(*Processor) {
	return func(p *Processor) {
		p.conditionFn = conditionFn
	}
}

type ProcessFn func(jetstream.Msg) error

func NewProcessor(consumer jetstream.Consumer, processFn ProcessFn, opts ...func(*Processor)) *Processor {
	p := &Processor{
		consumer:     consumer,
		processFn:    processFn,
		fetchTimeout: defaultFetchTimeout,
		conditionFn:  nil, // No condition check by default
	}

	// Apply any custom options
	for _, opt := range opts {
		opt(p)
	}

	return p
}

func (p *Processor) Process() {
	slog.Info("Looping...")
	// Continuously attempt to fetch and process messages.
	for {
		// Attempt to fetch the next message with a maximum wait time.
		msg, err := p.consumer.Next(jetstream.FetchMaxWait(p.fetchTimeout))
		if err != nil {
			slog.Warn("Failed fetch messages!", "err", err)
			continue
		}
		slog.Info("Received a JetStream message", "subject", msg.Subject())
		slog.Info("Processing message", "data", string(msg.Data()))

		// Check if condition function is set and evaluate it
		if p.conditionFn != nil {
			result, err := p.conditionFn(msg)
			if err != nil {
				slog.Error("Error checking condition", "err", err)
				p.handleErr(err, msg)
				continue
			}

			// Handle the condition result using the utility function
			shouldProcess, err := HandleConditionResult(msg, result)
			if err != nil {
				p.handleErr(err, msg)
				continue
			}

			if !shouldProcess {
				continue
			}
		}

		// Process the message
		err = p.processFn(msg)
		if err != nil {
			p.handleErr(err, msg)
		} else {
			// Acknowledge the message
			msg.Ack()
		}
	}
}
