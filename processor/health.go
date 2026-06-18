package processor

import (
	"fmt"
	"net/http"

	"github.com/nats-io/nats.go"
)

// ConnChecker is the subset of *nats.Conn needed by health checks.
type ConnChecker interface {
	IsClosed() bool
	IsDraining() bool
}

var _ ConnChecker = (*nats.Conn)(nil)

// HealthzHandler returns an HTTP handler suitable for a Kubernetes
// liveness probe. It reports 503 when the NATS connection is
// permanently closed or draining, or when any processor's fetch loop
// has exited — states a restart can recover from. A temporarily
// disconnected (reconnecting) connection is still considered healthy,
// since the client recovers on its own and a restart would not help.
func HealthzHandler(nc ConnChecker, procs ...*Processor) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if nc == nil || nc.IsClosed() {
			http.Error(w, "nats connection closed", http.StatusServiceUnavailable)
			return
		}
		if nc.IsDraining() {
			http.Error(w, "nats connection draining", http.StatusServiceUnavailable)
			return
		}
		for i, p := range procs {
			if p.Stopped() {
				http.Error(w, fmt.Sprintf("processor %d stopped", i), http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(http.StatusText(http.StatusOK)))
	}
}
