package processor

import (
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Processor represents the state and configuration of a NATS processor.
type Processor struct {
	consumer  jetstream.Consumer
	processFn ProcessFn
}

type ProcessFn func(jetstream.Msg) error

func NewProcessor(consumer jetstream.Consumer, processFn ProcessFn) *Processor {
	return &Processor{
		consumer:  consumer,
		processFn: processFn,
	}
}

func (p *Processor) Process() {
	slog.Info("Looping...")
	// Continuously attempt to fetch and process messages.
	for {
		// Attempt to fetch the next message with a maximum wait time.
		msg, err := p.consumer.Next(jetstream.FetchMaxWait(60 * time.Second))
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
