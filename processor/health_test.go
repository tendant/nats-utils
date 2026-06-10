package processor

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// stubConsumer implements jetstream.Consumer for tests; only Next is used by Processor.
type stubConsumer struct {
	next func() (jetstream.Msg, error)
}

func (s *stubConsumer) Next(opts ...jetstream.FetchOpt) (jetstream.Msg, error) { return s.next() }
func (s *stubConsumer) Fetch(batch int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	panic("not implemented")
}
func (s *stubConsumer) FetchBytes(maxBytes int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	panic("not implemented")
}
func (s *stubConsumer) FetchNoWait(batch int) (jetstream.MessageBatch, error) {
	panic("not implemented")
}
func (s *stubConsumer) Consume(handler jetstream.MessageHandler, opts ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	panic("not implemented")
}
func (s *stubConsumer) Messages(opts ...jetstream.PullMessagesOpt) (jetstream.MessagesContext, error) {
	panic("not implemented")
}
func (s *stubConsumer) Info(ctx context.Context) (*jetstream.ConsumerInfo, error) {
	panic("not implemented")
}
func (s *stubConsumer) CachedInfo() *jetstream.ConsumerInfo { panic("not implemented") }

func TestLastActivityUpdatedOnEachFetchIteration(t *testing.T) {
	calls := 0
	c := &stubConsumer{next: func() (jetstream.Msg, error) {
		calls++
		if calls < 3 {
			return nil, nats.ErrTimeout // non-terminal: loop continues
		}
		return nil, nats.ErrConnectionClosed
	}}
	p := NewProcessor(c, func(m jetstream.Msg) error { return nil })

	if !p.LastActivity().IsZero() {
		t.Fatal("LastActivity() must be zero before Run")
	}

	start := time.Now()
	p.Run()

	if p.LastActivity().Before(start) {
		t.Fatalf("LastActivity() = %v, must be at or after Run start %v", p.LastActivity(), start)
	}
}

func TestStoppedFlagSetWhenRunReturns(t *testing.T) {
	c := &stubConsumer{next: func() (jetstream.Msg, error) { return nil, nats.ErrConnectionClosed }}
	p := NewProcessor(c, func(m jetstream.Msg) error { return nil })

	if p.Stopped() {
		t.Fatal("Stopped() must be false before Run")
	}

	err := p.Run()
	if err == nil {
		t.Fatal("Run must return the terminal fetch error")
	}
	if !p.Stopped() {
		t.Fatal("Stopped() must be true after Run returns")
	}
}
