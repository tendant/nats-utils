package processor

import (
	"context"
	"net/http"
	"net/http/httptest"
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

type stubConn struct{ closed, draining bool }

func (s stubConn) IsClosed() bool   { return s.closed }
func (s stubConn) IsDraining() bool { return s.draining }

// stoppedProcessor returns a Processor whose Run loop has already exited.
func stoppedProcessor() *Processor {
	c := &stubConsumer{next: func() (jetstream.Msg, error) { return nil, nats.ErrConnectionClosed }}
	p := NewProcessor(c, func(m jetstream.Msg) error { return nil })
	p.Run()
	return p
}

func TestHealthzHandler(t *testing.T) {
	running := NewProcessor(&stubConsumer{}, func(m jetstream.Msg) error { return nil })

	cases := []struct {
		name string
		nc   ConnChecker
		proc *Processor
		want int
	}{
		{"healthy", stubConn{}, running, http.StatusOK},
		{"connection closed", stubConn{closed: true}, running, http.StatusServiceUnavailable},
		{"connection draining", stubConn{draining: true}, running, http.StatusServiceUnavailable},
		{"processor stopped", stubConn{}, stoppedProcessor(), http.StatusServiceUnavailable},
		{"nil connection", nil, running, http.StatusServiceUnavailable},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
			HealthzHandler(tc.nc, tc.proc)(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d (body: %q)", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

func TestProcessExitsProcessOnTerminalError(t *testing.T) {
	exitCode := -1
	origExit := osExit
	osExit = func(code int) { exitCode = code }
	defer func() { osExit = origExit }()

	c := &stubConsumer{next: func() (jetstream.Msg, error) { return nil, nats.ErrConnectionClosed }}
	p := NewProcessor(c, func(m jetstream.Msg) error { return nil })
	p.Process()

	if exitCode != 1 {
		t.Fatalf("Process() must exit with code 1 on terminal error, got %d", exitCode)
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
