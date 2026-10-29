# Observability

The Unstructured Data Controller and MCP Server include built-in observability via [OpenTelemetry](https://opentelemetry.io/). Both binaries expose metrics through a Prometheus-compatible `/metrics` endpoint and can optionally push telemetry to an OTel Collector via OTLP gRPC.

## How It Works

Both binaries use the shared `pkg/telemetry` package which initializes the OTel SDK with dual export:

- **Prometheus export (always on)** -- custom metrics are served on a `/metrics` HTTP endpoint in Prometheus format. For the controller, they appear alongside controller-runtime's built-in metrics (work queue depth, reconcile counts, etc.). For the MCP server, a dedicated `/metrics` endpoint is exposed on a separate port (8000) to isolate it from the MCP protocol traffic on port 8080.

- **OTLP push (opt-in)** -- when `OTEL_EXPORTER_OTLP_ENDPOINT` is set, metrics and traces are pushed to an OTel Collector via gRPC. The collector can then forward to any backend (Datadog, Langfuse, Jaeger, etc.).

```
Controller ──┐                              ┌── Datadog
             ├── OTLP gRPC ──> OTel Collector ──> Langfuse
MCP Server ──┘                              └── (any OTLP backend)
             │
             └── /metrics ──> Prometheus / any scraper
```

## Configuration

All configuration uses standard [OTel environment variables](https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/) -- no custom flags needed.

| Variable | Description | Default |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTel Collector endpoint (e.g. `http://otel-collector:4317`). Enables OTLP push when set. | unset (disabled) |
| `OTEL_EXPORTER_OTLP_INSECURE` | Disable TLS for the collector connection (plaintext gRPC). An `http://` endpoint scheme also disables TLS. | `false` |
| `OTEL_SERVICE_NAME` | Override the default service name in telemetry data. | set by the binary |
| `OTEL_RESOURCE_ATTRIBUTES` | Additional resource attributes, as `key=value` pairs. | unset |
| `OTEL_SDK_DISABLED` | Set to `true` to turn telemetry off without a redeploy. No Prometheus collector is registered and no OTLP connection is opened. | `false` |
| `OTEL_TRACES_SAMPLER` | Sampler name (e.g. `always_off`, `traceidratio`). When set, it replaces the binary's built-in sampler entirely. | unset (binary's own ratio applies) |
| `OTEL_TRACES_SAMPLER_ARG` | Sampling ratio between 0 and 1. | binary's own ratio |

Not yet supported: the per-signal endpoint variables (`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`) and `OTEL_EXPORTER_OTLP_PROTOCOL`. OTLP is gated on `OTEL_EXPORTER_OTLP_ENDPOINT` alone and always uses gRPC.

### Enabling OTLP push

**Controller** -- uncomment the env vars in `config/manager/manager.yaml`:

```yaml
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: "http://otel-collector.observability.svc:4317"
- name: OTEL_EXPORTER_OTLP_INSECURE
  value: "true"
```

**MCP Server** -- uncomment in `config/mcp/configmap.yaml`:

```yaml
OTEL_EXPORTER_OTLP_ENDPOINT: "http://otel-collector.observability.svc:4317"
OTEL_EXPORTER_OTLP_INSECURE: "true"
```

### Prometheus scraping

The controller's metrics endpoint is managed by controller-runtime (port 8443, HTTPS with authn/authz). See the [kubebuilder metrics reference](https://book.kubebuilder.io/reference/metrics) for configuration.

The MCP server's `/metrics` endpoint is on a dedicated port 8000 (HTTP, unauthenticated), isolated from the MCP protocol on port 8080. The deployment includes standard Prometheus scrape annotations:

```yaml
annotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "8000"
  prometheus.io/path: "/metrics"
```

### SDK diagnostics

Both binaries pass a `Logger` in `telemetry.Config`, so OTel's own failures -- a refused OTLP connection, an expired collector certificate, dropped spans -- are reported through the same structured log pipeline as everything else, under the `otel` logger name. Without this the SDK writes them to a bare `log.Print` on stderr, and a broken exporter looks like flat dashboards with healthy, Ready pods and nothing in the logs.

## Metrics Reference

### Controller metrics

Recorded per reconcile loop. All metrics include a `controller` attribute identifying which controller emitted them.

| Metric | Type | Unit | Description |
|---|---|---|---|
| `udc.reconcile.count` | Counter | reconcile | Total reconcile invocations |
| `udc.reconcile.duration` | Histogram | seconds | Wall-clock duration of a reconcile loop |
| `udc.reconcile.errors` | Counter | error | Reconcile invocations that returned an error |
| `udc.files.processed` | Counter | file | Cumulative files processed across reconciles |

Prometheus names follow OTel conventions (dots become underscores, counters get `_total` suffix):

```
udc_reconcile_count_total{controller="DocumentProcessor"} 42
udc_reconcile_duration_seconds_bucket{controller="SourceCrawler",le="1"} 15
udc_files_processed_total{controller="ChunksGenerator"} 1200
```

### MCP Server metrics

Recorded per tool invocation.

| Metric | Type | Unit | Description |
|---|---|---|---|
| `udc.mcp.tool.calls` | Counter | call | Total MCP tool invocations |
| `udc.mcp.tool.duration` | Histogram | seconds | Tool invocation wall-clock duration |
| `udc.mcp.tool.errors` | Counter | error | Tool invocations that returned an error |
| `udc.mcp.external.duration` | Histogram | seconds | Duration of external service calls (K8s, Snowflake, embedding API) |

Tool metrics include a `tool` attribute. External call metrics include `service`, `operation`, and `outcome` (`success` or `error`) attributes.

`udc.mcp.external.duration` is the only external-call instrument, so `outcome` is what makes a dependency failure visible in metrics. Alert on the error share rather than on latency alone, since a dependency that fails fast lowers p99:

```
sum by (service) (rate(udc_mcp_external_duration_seconds_count{outcome="error"}[5m]))
  / sum by (service) (rate(udc_mcp_external_duration_seconds_count[5m]))
```

### MCP Server traces

When OTLP is enabled, the MCP server emits traces with spans for each tool invocation and child spans for external service calls. The tool span is started by `TelemetryMiddleware` (see below), so its name is always the name the tool was registered under. Child spans are created in `pkg/` packages via the global `otel.Tracer()` API and propagate automatically through context.

**get_chunks_for_embeddings:**
```
mcp.tool/get_chunks_for_embeddings       [root, attrs: mcp.pipeline_name]
  |-- k8s.get_pipeline_query_config      [attrs: pipeline.name, stage.type]
  |-- embedding.generate                 [attrs: embedding.model, embedding.input_count]
  |-- snowflake.search_chunks            [attrs: db.system.name, db.namespace]
```

**list_unstructured_data_pipelines_for_user:**
```
mcp.tool/list_unstructured_data_pipelines_for_user   [root]
  |-- k8s.list_pipelines
  |-- snowflake.show_databases           [attrs: db.system.name]
```

**get_processed_document:**
```
mcp.tool/get_processed_document          [root, attrs: mcp.pipeline_name, mcp.file_id]
  |-- k8s.get_pipeline_query_config      [attrs: pipeline.name, stage.type]
  |-- snowflake.get_processed_document   [attrs: db.system.name, db.namespace]
```

Traces are useful for Langfuse integration (LLM observability) and debugging latency in the tool call chain.

## Adding Instrumentation to New Code

### New controller

Add a `Metrics *udctelemetry.Provider` field to the reconciler struct and wrap
the reconciler with `ObserveReconciler` in `SetupWithManager`. `Reconcile`
itself needs no telemetry code:

```go
type MyReconciler struct {
    client.Client
    Scheme *runtime.Scheme
    // Metrics records reconcile telemetry. Nil disables recording.
    Metrics *udctelemetry.Provider
}

func (r *MyReconciler) SetupWithManager(mgr ctrl.Manager) error {
    return ctrl.NewControllerManagedBy(mgr).
        For(&operatorv1alpha1.MyResource{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
        Named("myresource").
        Complete(r.Metrics.ObserveReconciler(MyControllerName, r))
}
```

Decorating the reconciler rather than deferring inside `Reconcile` also counts
panics. controller-runtime's `recover()` sits *above* the reconciler, so a
panicking reconcile unwinds past an in-method defer while the named error
return is still nil, and would be recorded as a success.

To track files processed, call `RecordFilesProcessed` *after* the status patch
succeeds, so a failed patch followed by a retry does not double-count:

```go
if err := controllerutils.StatusPatch(ctx, r.Client, cr, func() {
    cr.Status.FilesProcessed += count
}); err != nil {
    return r.handleError(ctx, cr, err)
}
r.Metrics.RecordFilesProcessed(ctx, MyControllerName, count)
```

Wire the provider in `cmd/main.go` when constructing the reconciler. The
recording methods tolerate a nil `Provider`, so no nil guards are needed at
recording sites and reconcilers work unchanged when telemetry is disabled in
tests.

### New MCP tool

Call count, duration, errors, and the tool span are recorded by
`tools.TelemetryMiddleware`, registered once on the server in
`cmd/unstructured-data-mcp-server/main.go`:

```go
mcpServer.AddReceivingMiddleware(mcptools.TelemetryMiddleware(metricsProvider))
```

A new tool is therefore instrumented as soon as it is registered, under the
name it was registered with, and nothing needs adding to the handler. The
middleware sees arguments only as raw JSON, so a handler that wants them on
the span adds them itself. Take a `*udctelemetry.Provider` parameter only if
the tool makes external calls worth timing:

```go
func RegisterMyTool(s *mcp.Server, metricsProvider *udctelemetry.Provider) {
    mcp.AddTool(s, &mcp.Tool{...},
        func(ctx context.Context, _ *mcp.CallToolRequest, args myToolArgs) (*mcp.CallToolResult, any, error) {
        // Enrich the span the middleware already started.
        trace.SpanFromContext(ctx).SetAttributes(
            attribute.String("mcp.pipeline_name", args.PipelineName))

        // Time external calls:
        start := time.Now()
        result, err := externalService.Call(ctx, ...)
        metricsProvider.RecordExternalCall(ctx, "service_name", "operation", time.Since(start), err)
        if err != nil {
            return errorResult, nil, nil
        }
        // ...
    })
}
```

A handler reports failure through `CallToolResult.IsError` rather than a Go
error, so the middleware inspects both the result and the returned error to
decide whether a call failed. It also counts a panicking handler as a failure.

### Recording errors on spans

Never call `span.RecordError` directly. Downstream errors routinely embed
connection strings, query text, or HTTP response bodies, and span attributes
are exported to Datadog and Langfuse. Use the sanitizing helper instead:

```go
defer func() {
    udctelemetry.RecordSpanError(span, err)
    span.End()
}()
```

`RecordSpanError` redacts credential material (URL userinfo including passwords
containing `@`, bearer/basic tokens, and `password=`/`token=`/`api_key` pairs)
and truncates oversized messages before marking the span failed. A nil error is
a no-op, so the call can be deferred unconditionally.

The same sanitization runs inside `TelemetryMiddleware` and
`RecordExternalCall`, so every span this project writes an error to goes
through it.

Sanitization covers the span path only. The same error is usually also logged
and, in MCP handlers, returned to the client in the tool result -- neither is
redacted, so do not rely on this as the only control when handling an error
that may carry credential material.

### New pkg/ function with external calls

Add a trace span using the global `otel.Tracer()` API -- the span propagates
automatically when the caller has an active span, so there is no need to
thread a provider through. Record the error with `RecordSpanError`, which
means the package does take a `pkg/telemetry` import:

```go
func MyExternalCall(ctx context.Context, ...) (result T, err error) {
    ctx, span := otel.Tracer("pkg/mypackage").Start(ctx, "mypackage.operation",
        trace.WithSpanKind(trace.SpanKindClient),
        trace.WithAttributes(attribute.String("key", "value")))
    defer func() {
        udctelemetry.RecordSpanError(span, err)
        span.End()
    }()
    // ... existing logic ...
}
```

## Testing Locally

### Verify Prometheus metrics (no collector needed)

```bash
# Build and run the MCP server
go build -o /tmp/mcp-server ./cmd/unstructured-data-mcp-server

SSO_AUTHORIZATION_URL=http://localhost \
SSO_TOKEN_URL=http://localhost \
SSO_CALLBACK_URL=http://localhost \
SSO_DISABLE_INTROSPECTION=true \
/tmp/mcp-server &

# Check for custom metrics
curl -s http://localhost:8000/metrics | grep udc_
```

### Verify OTLP push with a local collector

```bash
# Start a local OTel Collector
docker run --rm -p 4317:4317 -p 55679:55679 \
  otel/opentelemetry-collector-contrib:latest

# Run the MCP server with OTLP enabled
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
OTEL_EXPORTER_OTLP_INSECURE=true \
SSO_AUTHORIZATION_URL=http://localhost \
SSO_TOKEN_URL=http://localhost \
SSO_CALLBACK_URL=http://localhost \
SSO_DISABLE_INTROSPECTION=true \
/tmp/mcp-server
```

The collector logs will show received metric data points.

### Unit tests

```bash
make test
```

## Architecture Decisions

**Why OTel SDK instead of Prometheus client?** The project needs to support both pull (Prometheus scraping for OSS users) and push (OTLP to OTel Collector for Datadog/Langfuse). The OTel SDK supports both via its Prometheus exporter and OTLP exporter. This follows the same pattern as [Tekton](https://tekton.dev/) and [Knative](https://knative.dev/), which are the only major Kubernetes projects that use the OTel SDK natively.

**Why not the Prometheus-to-OTel bridge?** The bridge adds app-side overhead and silently drops Summary metrics. Since this is a new instrumentation effort (no existing Prometheus metrics to migrate), writing directly against the OTel SDK avoids the translation layer entirely. Controller-runtime's built-in Prometheus metrics remain on the `/metrics` endpoint as-is.

**Why keep every OTel module on one version?** The SDK, the API, and the exporters share internal packages, so mixing versions across `go.opentelemetry.io/otel*` produces build failures and silently inconsistent behaviour. All of them are held at `v1.47.0`, except the Prometheus exporter, which tracks the same releases on its own pre-1.0 line (`v0.69.0`), and `contrib/instrumentation/net/http/otelhttp` (`v0.70.0`). Moving to v1.47.0 pulled a set of transitive upgrades with it (gRPC, protobuf, `golang.org/x/net`, `golang.org/x/crypto`, `go-openapi`, and `cenkalti/backoff` v4 to v5); the alternative, holding OTel back to the version controller-runtime pins, would have meant running exporters older than the API they are compiled against.
