package processor

import (
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const defaultFetchTimeout = 60 * time.Second

// Processor represents the state and configuration of a NATS processor.
type Processor struct {
	consumer     jetstream.Consumer
	processFn    ProcessFn
	fetchTimeout time.Duration
}

// WithFetchTimeout sets the timeout duration for fetching messages.
// If not set, defaults to 60 seconds.
func WithFetchTimeout(timeout time.Duration) func(*Processor) {
	return func(p *Processor) {
		p.fetchTimeout = timeout
	}
}

type ProcessFn func(jetstream.Msg) error

func NewProcessor(consumer jetstream.Consumer, processFn ProcessFn, opts ...func(*Processor)) *Processor {
	p := &Processor{
		consumer:     consumer,
		processFn:    processFn,
		fetchTimeout: defaultFetchTimeout,
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
			// return
			continue
		}
		slog.Info("Received a JetStream message", "subject", msg.Subject())
		slog.Info("Processing message", "data", string(msg.Data()))

		err = p.processFn(msg)
		if err != nil {
			p.handleErr(err, msg)
		} else {
			msg.Ack()
		}
	}

}
