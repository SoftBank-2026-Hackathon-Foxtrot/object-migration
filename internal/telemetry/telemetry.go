// Package telemetry exports bounded migration observations through the official OTEL SDK.
package telemetry

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type sample struct {
	name  string
	value float64
	attrs []attribute.KeyValue
	at    time.Time
}

type Telemetry struct {
	values   map[string]sample
	logs     *sdklog.LoggerProvider
	metrics  *sdkmetric.MeterProvider
	traces   *sdktrace.TracerProvider
	logger   log.Logger
	tracer   trace.Tracer
	gauges   map[string]metric.Float64ObservableGauge
	duration metric.Float64Histogram
	mu       sync.Mutex
}

func New(ctx context.Context) (*Telemetry, error) {
	t := &Telemetry{tracer: noop.NewTracerProvider().Tracer("foxtrot.migration"), gauges: map[string]metric.Float64ObservableGauge{}, values: map[string]sample{}}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" || os.Getenv("OTEL_SDK_DISABLED") == "true" {
		return t, nil
	}
	name := os.Getenv("OTEL_SERVICE_NAME")
	if name == "" {
		name = "foxtrot-object"
	}
	res, err := resource.New(ctx, resource.WithFromEnv(), resource.WithAttributes(
		attribute.String("service.name", name), attribute.String("service.instance.id", time.Now().UTC().Format(time.RFC3339Nano)),
		attribute.String("foxtrot.module", "object"), attribute.String("foxtrot.contract.version", "1")))
	if err != nil {
		return t, err
	}
	le, err := otlploghttp.New(ctx, otlploghttp.WithTimeout(2*time.Second))
	if err != nil {
		return t, err
	}
	t.logs = sdklog.NewLoggerProvider(sdklog.WithResource(res), sdklog.WithProcessor(sdklog.NewBatchProcessor(le, sdklog.WithMaxQueueSize(2048))))
	me, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithTimeout(2*time.Second))
	if err != nil {
		_ = t.Close(ctx)
		return t, err
	}
	t.metrics = sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(sdkmetric.NewPeriodicReader(me, sdkmetric.WithInterval(5*time.Second), sdkmetric.WithTimeout(3*time.Second))))
	te, err := otlptracehttp.New(ctx, otlptracehttp.WithTimeout(2*time.Second))
	if err != nil {
		_ = t.Close(ctx)
		return t, err
	}
	t.traces = sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithBatcher(te, sdktrace.WithMaxQueueSize(2048)))
	t.logger, t.tracer = t.logs.Logger("foxtrot.migration"), t.traces.Tracer("foxtrot.migration")
	t.duration, _ = t.metrics.Meter("foxtrot.migration").Float64Histogram("foxtrot.operation.duration", metric.WithUnit("s"))
	return t, nil
}

func (t *Telemetry) Close(ctx context.Context) error {
	if t.logs != nil {
		_ = t.logs.Shutdown(ctx)
	}
	if t.metrics != nil {
		_ = t.metrics.Shutdown(ctx)
	}
	if t.traces != nil {
		_ = t.traces.Shutdown(ctx)
	}
	return nil
}

func (t *Telemetry) Measure(ctx context.Context, name string, value float64, attrs ...attribute.KeyValue) {
	if t.metrics == nil {
		return
	}
	encoded, _ := json.Marshal(attrs)
	key := name + string(encoded)
	t.mu.Lock()
	for k, v := range t.values {
		if time.Since(v.at) > 20*time.Second {
			delete(t.values, k)
		}
	}
	if _, ok := t.values[key]; !ok && len(t.values) >= 2048 {
		t.mu.Unlock()
		return
	}
	t.values[key] = sample{name: name, value: value, attrs: attrs, at: time.Now()}
	_, exists := t.gauges[name]
	if !exists {
		t.gauges[name] = nil
	}
	t.mu.Unlock()
	if !exists {
		gauge, _ := t.metrics.Meter("foxtrot.migration").Float64ObservableGauge(name, metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			t.mu.Lock()
			defer t.mu.Unlock()
			for _, v := range t.values {
				if v.name == name && time.Since(v.at) <= 20*time.Second {
					o.Observe(v.value, metric.WithAttributes(v.attrs...))
				}
			}
			return nil
		}))
		t.mu.Lock()
		t.gauges[name] = gauge
		t.mu.Unlock()
	}
}

func (t *Telemetry) Snapshot(ctx context.Context, workflow string, data any) {
	if t.logger == nil || workflow == "" || len(workflow) > 128 {
		return
	}
	body, err := json.Marshal(map[string]any{"schemaVersion": 1, "resource": "OBJECT_STORAGE", "data": data, "collectionError": nil})
	if err != nil || len(body) > 32768 {
		return
	}
	var record log.Record
	record.SetTimestamp(time.Now())
	record.SetSeverity(log.SeverityInfo)
	record.SetBody(attribute.StringValue(string(body)))
	record.AddAttributes(attribute.String("foxtrot.event", "snapshot"), attribute.String("workflow.id", workflow), attribute.String("resource.kind", "OBJECT_STORAGE"))
	t.logger.Emit(ctx, record)
}

func Headers(ctx context.Context) map[string]string {
	h := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, h)
	return h
}

func (t *Telemetry) Operation(ctx context.Context, workflow, migration, operation, action string, parent map[string]string) (context.Context, func(error)) {
	if len(parent) > 0 {
		ctx = propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier(parent))
	}
	started := time.Now()
	ctx, span := t.tracer.Start(ctx, "migration."+action, trace.WithAttributes(attribute.String("workflow.id", workflow), attribute.String("migration.id", migration), attribute.String("operation.id", operation), attribute.String("migration.action", action)))
	return ctx, func(err error) {
		result := "ok"
		if err != nil {
			result = "error"
			span.SetStatus(codes.Error, "OPERATION_UNCONFIRMED")
		}
		if t.duration != nil {
			t.duration.Record(ctx, time.Since(started).Seconds(), metric.WithAttributes(attribute.String("migration.action", action), attribute.String("result", result)))
		}
		span.End()
	}
}

func (t *Telemetry) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := propagation.TraceContext{}.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := t.tracer.Start(ctx, r.Method, trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()
		req := r.WithContext(ctx)
		next.ServeHTTP(w, req)
		span.SetAttributes(attribute.String("http.route", req.Pattern))
		span.SetName(r.Method + " " + req.Pattern)
	})
}
