package processor

import (
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const defaultFetchTimeout = 60 * time.Second

// ConditionCheckFn is a function that checks if conditions are met for processing a message
type ConditionCheckFn func(jetstream.Msg) (bool, error)

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

		// Check if we have a condition function and if conditions are met
		if p.conditionFn != nil {
			conditionMet, err := p.conditionFn(msg)
			if err != nil {
				slog.Error("Error checking condition", "err", err)
				p.handleErr(err, msg)
				continue
			}

			if !conditionMet {
				// Condition not met, check metadata for retry count
				metadata, err := msg.Metadata()
				if err != nil {
					slog.Error("Error getting message metadata", "err", err)
					p.handleErr(err, msg)
					continue
				}

				// Check if we've exceeded max retries
				if metadata.NumDelivered > uint64(p.maxRetries) {
					slog.Warn("Max retries exceeded, terminating message", "msgID", metadata.Stream)
					// Acknowledge the message to remove it from the queue
					msg.Ack()
					continue
				}

				// Condition not met and under retry limit, do not ack so it will be redelivered
				slog.Info("Condition not met, message will be reprocessed later",
					"delivery", metadata.NumDelivered,
					"maxRetries", p.maxRetries)

				// Let the message go back to the stream without acknowledgment
				continue
			}

			// Condition met, proceed with processing
			slog.Info("Condition met, processing message")
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
