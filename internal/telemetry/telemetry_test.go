package telemetry

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracespb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestOfficialSDKExportsThreeOTLPSignals(t *testing.T) {
	var mu sync.Mutex
	received := map[string][]byte{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		received[r.URL.Path] = b
		mu.Unlock()
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(200)
	}))
	defer srv.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", srv.URL)
	t.Setenv("OTEL_SDK_DISABLED", "false")
	ctx := context.Background()
	otel, err := New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ctx, end := otel.Operation(ctx, "wf", "migration", "op", "COPY", nil)
	child, done := otel.Operation(context.Background(), "wf", "migration", "op", "VERIFY", Headers(ctx))
	otel.Snapshot(child, "wf", map[string]any{"workflowId": "wf", "migrationId": "migration"})
	otel.Measure(ctx, "foxtrot.migration.completed", 7, attribute.String("workflow.id", "wf"))
	done(nil)
	end(nil)
	_ = otel.logs.ForceFlush(ctx)
	_ = otel.metrics.ForceFlush(ctx)
	_ = otel.traces.ForceFlush(ctx)
	defer otel.Close(ctx)
	mu.Lock()
	defer mu.Unlock()
	var logs logspb.ExportLogsServiceRequest
	if err := proto.Unmarshal(received["/v1/logs"], &logs); err != nil || len(logs.ResourceLogs) != 1 {
		t.Fatalf("logs: %v", err)
	}
	record := logs.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
	if len(record.TraceId) != 16 {
		t.Fatal("snapshot lost active trace context")
	}
	var metrics metricspb.ExportMetricsServiceRequest
	if err := proto.Unmarshal(received["/v1/metrics"], &metrics); err != nil || len(metrics.ResourceMetrics) != 1 {
		t.Fatalf("metrics: %v", err)
	}
	var traces tracespb.ExportTraceServiceRequest
	if err := proto.Unmarshal(received["/v1/traces"], &traces); err != nil || len(traces.ResourceSpans) != 1 {
		t.Fatalf("traces: %v", err)
	}
	spans := traces.ResourceSpans[0].ScopeSpans[0].Spans
	if len(spans) != 2 || string(spans[0].ParentSpanId) != string(spans[1].SpanId) {
		t.Fatal("W3C parent context was not propagated")
	}
}
