# Observability

The Unstructured Data Controller and MCP Server include built-in observability via [OpenTelemetry](https://opentelemetry.io/). Telemetry is pushed to an OTel Collector over OTLP gRPC, which fans out to Datadog, Langfuse, or any OTLP backend.

## How It Works

Both binaries use the shared `pkg/telemetry` package, which initializes the OTel SDK with two export paths. They differ in which one is actually reachable today:

- **OTLP push** -- when `OTEL_EXPORTER_OTLP_ENDPOINT` is set, metrics and traces go to an OTel Collector over gRPC. This is the delivery path for both binaries.

- **Prometheus pull** -- the SDK always builds a Prometheus exporter, but only the MCP server serves it, on a dedicated port (8000, HTTP) kept separate from the MCP protocol traffic on 8080. **A default install does not expose a controller metrics endpoint**: `config/deploy` leaves `--metrics-bind-address` at its default of `0`, so its `udc_*` instruments are registered on controller-runtime's registry but nothing serves them. Exposing the controller's endpoint is deliberately left to a follow-up change; until then, set `OTEL_EXPORTER_OTLP_ENDPOINT` or the controller emits nothing.

```
Controller ── OTLP/gRPC ─┐                      ┌── Datadog   (metrics + traces)
                         ├── OTel Collector ────┤
MCP Server ── OTLP/gRPC ─┘                      └── Langfuse  (traces, via HTTP)

MCP Server ── /metrics :8000 ──> Prometheus / any scraper
```

### Routing to Datadog and Langfuse

Both binaries speak OTLP/gRPC to a collector; the collector fans out to backends. Two things are worth knowing before wiring this up.

**Langfuse consumes traces, not metrics.** It is an LLM-observability backend, so what it renders is the MCP server's tool spans and their children (embedding, Snowflake, K8s). The MCP server samples at 1.0 for exactly this reason. Its native OTLP ingest is HTTP/protobuf, and only gRPC exporters are compiled in here, so Langfuse has to sit behind a collector -- point the app at the collector over gRPC and let the collector's exporter talk HTTP to Langfuse. Direct app-to-Langfuse will not connect.

**Only `udc_*` metrics travel over OTLP.** controller-runtime's own metrics -- reconcile totals, workqueue depth and latency, client request counts -- are native Prometheus collectors on its registry, not OTel instruments, so the OTLP exporter never sees them. Workqueue depth is usually the first thing you want in Datadog when a controller stalls, and it is not currently reachable by any path: the controller serves no metrics endpoint for a collector to scrape. Getting those into Datadog needs the follow-up that exposes the endpoint, plus a `prometheus` receiver on the collector alongside the OTLP one.

Keeping that claim true takes some care. `telemetry.Init` installs the OTel SDK as the process-wide MeterProvider, which switches on any OTel-aware library in the binary, not just this package -- `google.golang.org/api` wraps its transport in `otelhttp` and resolves the global at construction. `pkg/gdrive` therefore passes `option.WithTelemetryDisabled()` on every client it builds; without it the operator would silently export `http.client.*` series, one set per host x method x status code. A new dependency that instruments itself off the global will need the same treatment.

**The `controller` label is shared.** `udc_reconcile_*` and `controller_runtime_reconcile_*` are served from the same registry and use the same label value, so they can be joined -- `Named()` and the metric label both come from one constant per reconciler. The values are lowercase because that is what controller-runtime already emits; changing them would move an existing label.

## Configuration

All configuration uses standard [OTel environment variables](https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/) -- no custom flags needed.

| Variable | Description | Default |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTel Collector endpoint (e.g. `http://otel-collector:4317`). Enables OTLP push when set. | unset (disabled) |
| `OTEL_EXPORTER_OTLP_INSECURE` | Disable TLS for the collector connection (plaintext gRPC). An `http://` endpoint scheme also disables TLS. | `false` |
| `OTEL_SERVICE_NAME` | Override the default service name in telemetry data. | set by the binary |
| `OTEL_RESOURCE_ATTRIBUTES` | Additional resource attributes, as `key=value` pairs. | unset |
| `OTEL_SDK_DISABLED` | Set to `true` to turn telemetry off. No Prometheus collector is registered and no OTLP connection is opened. Read once at startup, so changing it needs a pod restart or rollout -- it is not a live kill switch. | `false` |
| `OTEL_TRACES_SAMPLER` | Sampler name (e.g. `always_off`, `traceidratio`). When set, it replaces the binary's built-in sampler entirely. Only has an effect alongside `OTEL_EXPORTER_OTLP_ENDPOINT`, since no tracer is built without one. | unset (binary's own ratio applies) |
| `OTEL_TRACES_SAMPLER_ARG` | Sampling ratio between 0 and 1. Same caveat as above. | binary's own ratio |

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

**A default install has no controller metrics endpoint to scrape.** `config/deploy`, the overlay `make deploy` builds, leaves `--metrics-bind-address` at its default of `0`, so controller-runtime serves nothing and neither the `udc_*` instruments nor controller-runtime's own metrics are readable by pull. OTLP push is the controller's only delivery path today. Exposing the endpoint is a planned follow-up; it needs more than flipping the flag:

- The scaffolded `config/default/manager_metrics_patch.yaml` inserts the flag at `args[0]`, **ahead of** `/manager`. This image's ENTRYPOINT is `entrypoint.sh`, which ends in `exec "$@"`, so `args[0]` is the binary to exec. Applying that patch as written makes the flag the command and the pod CrashLoopBackOffs. The flag has to be appended, not prepended.
- `config/deploy` (what `make deploy` builds) pulls in only `../rbac` and `../manager`, so it picks up neither that patch nor `config/default/metrics_service.yaml`.
- `--metrics-secure` defaults to true, so a scraper needs a bearer token plus `metrics_auth_role.yaml`, `metrics_auth_role_binding.yaml` and `metrics_reader_role.yaml` uncommented in `config/rbac/kustomization.yaml`. Those are cluster-scoped (`TokenReview`/`SubjectAccessReview`), a wider grant than the rest of this operator's namespace-scoped RBAC, so that is a deliberate decision rather than a default.

See the [kubebuilder metrics reference](https://book.kubebuilder.io/reference/metrics).

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
udc_reconcile_count_total{controller="documentprocessor"} 42
udc_reconcile_duration_seconds_bucket{controller="sourcecrawler",le="1"} 15
udc_files_processed_total{controller="chunksgenerator"} 1200
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
        |-- snowflake.query              [one per attempt]
```

With a `file_path_filter`, the handler calls `SearchChunksWithFileFilter`, which has no named span of its own; the `snowflake.query` span appears directly under the tool span instead.

**list_unstructured_data_pipelines_for_user:**
```
mcp.tool/list_unstructured_data_pipelines_for_user   [root]
  |-- k8s.list_pipelines
  |-- snowflake.show_databases           [attrs: db.system.name]
        |-- snowflake.query              [one per attempt]
```

**get_processed_document:**
```
mcp.tool/get_processed_document          [root, attrs: mcp.pipeline_name, mcp.file_id]
  |-- k8s.get_pipeline_query_config      [attrs: pipeline.name, stage.type]
  |-- snowflake.get_processed_document   [attrs: db.system.name, db.namespace]
        |-- snowflake.query              [one per attempt]
```

Every Snowflake call goes through `queryRows`, which retries up to three times with backoff, and each attempt gets its own `snowflake.query` span. Three of them under one parent means the query failed twice and the parent's duration is mostly backoff, not database time. The query text is not recorded: it carries table names and bound values, and spans leave the process.

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
    // Returned with context, not logged first: controller-runtime logs a
    // returned reconcile error, so doing both duplicates it. The existing
    // handleError does log-and-return; CLAUDE.md names that as a deviation
    // new code should not copy.
    return ctrl.Result{}, fmt.Errorf("updating %s status: %w", MyControllerName, err)
}
r.Metrics.RecordFilesProcessed(ctx, MyControllerName, count)
```

Record partial counts on early-return paths too. A reconcile that processes
some files and then fails still did that work, and the next attempt skips
those files as already done, so a count taken only on full success is lost
permanently rather than deferred.

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

Only the MCP server serves `/metrics` in a default install; the controller does not (see above). The
server needs a reachable Kubernetes API, so run it with a kubeconfig pointing at
a cluster or a `kind` instance - without one it exits before the listener binds.

```bash
make build-mcp-server   # writes bin/unstructured-data-mcp-server

KUBECONFIG=~/.kube/config \
SSO_AUTHORIZATION_URL=http://localhost \
SSO_TOKEN_URL=http://localhost \
SSO_CALLBACK_URL=http://localhost \
SSO_DISABLE_INTROSPECTION=true \
bin/unstructured-data-mcp-server &
```

A counter does not appear on `/metrics` until it has been incremented at least
once, so a freshly started server shows no `udc_` series. Exercise a tool first,
then scrape:

```bash
curl -s http://localhost:8000/metrics | grep udc_
```

The MCP server serves a dedicated registry that deliberately excludes Go and
process collectors, so before any tool has run the only series present is
`target_info`. Its presence is how you confirm the endpoint is up:

```bash
curl -s http://localhost:8000/metrics | grep target_info
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

**Why OTel SDK instead of Prometheus client?** The project needs to support both pull (Prometheus scraping for OSS users) and push (OTLP to OTel Collector for Datadog/Langfuse). The OTel SDK supports both via its Prometheus exporter and OTLP exporter, which is what keeps the controller's pull path a configuration change rather than a reinstrumentation. This follows the same pattern as [Tekton](https://tekton.dev/) and [Knative](https://knative.dev/), which are the only major Kubernetes projects that use the OTel SDK natively.

**Why not the Prometheus-to-OTel bridge?** The bridge adds app-side overhead and silently drops Summary metrics. Since this is a new instrumentation effort (no existing Prometheus metrics to migrate), writing directly against the OTel SDK avoids the translation layer entirely. Controller-runtime's built-in Prometheus metrics remain on the `/metrics` endpoint as-is.

**Why keep every OTel module on one version?** The SDK, the API, and the exporters share internal packages, so mixing versions across `go.opentelemetry.io/otel*` produces build failures and silently inconsistent behaviour. All of them are held at `v1.47.0`, except the Prometheus exporter, which tracks the same releases on its own pre-1.0 line (`v0.69.0`), and `contrib/instrumentation/net/http/otelhttp` (`v0.70.0`). Moving to v1.47.0 pulled a set of transitive upgrades with it (gRPC, protobuf, `golang.org/x/net`, `golang.org/x/crypto`, `go-openapi`, and `cenkalti/backoff` v4 to v5); the alternative, holding OTel back to the version controller-runtime pins, would have meant running exporters older than the API they are compiled against.
