// Package metrics exposes Prometheus collectors and HTTP instrumentation.
package metrics

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// HTTPRequestsTotal counts handled HTTP requests by method, route, and status.
	HTTPRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "clob_http_requests_total",
			Help: "Total HTTP requests handled by the gateway.",
		},
		[]string{"method", "route", "status"},
	)

	// HTTPRequestDuration measures request latency in seconds.
	HTTPRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "clob_http_request_duration_seconds",
			Help:    "HTTP request latency in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "route"},
	)

	// OrdersPlacedTotal counts orders accepted by the gateway, by market and side.
	OrdersPlacedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "clob_orders_placed_total",
			Help: "Orders accepted by the gateway.",
		},
		[]string{"market", "side"},
	)

	// WorkerEventsTotal counts events processed by a worker, by worker name and event type.
	WorkerEventsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "clob_worker_events_total",
			Help: "Events processed by workers.",
		},
		[]string{"worker", "event_type"},
	)

	// WorkerEventErrorsTotal counts worker event-processing failures.
	WorkerEventErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "clob_worker_event_errors_total",
			Help: "Worker event-processing failures.",
		},
		[]string{"worker"},
	)
)

// Handler returns the Prometheus scrape handler for the /metrics endpoint.
func Handler() http.Handler {
	return promhttp.Handler()
}

// statusRecorder captures the response status code for metrics.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// Hijack forwards to the underlying ResponseWriter so WebSocket upgrades
// (e.g. /v1/stream) still work behind this middleware. Without it, the embedded
// http.ResponseWriter interface hides Hijack and coder/websocket's Accept fails
// with 501 Not Implemented.
func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("metrics: underlying ResponseWriter is not an http.Hijacker")
	}
	return h.Hijack()
}

// Middleware records request count and latency for every HTTP request.
// Uses the chi route pattern (e.g. "/v1/orders/{id}") rather than the raw path
// so per-order URLs don't explode metric cardinality.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(rec, r)

		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = "unmatched"
		}

		HTTPRequestsTotal.WithLabelValues(r.Method, route, strconv.Itoa(rec.status)).Inc()
		HTTPRequestDuration.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
	})
}
