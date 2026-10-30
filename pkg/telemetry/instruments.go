/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package telemetry

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const meterName = "udc"

// instruments holds pre-registered OTel metric instruments for the controller
// and MCP server.
//
// The reconcile instruments deliberately duplicate controller-runtime's
// built-in controller_runtime_reconcile_{total,time_seconds,errors_total}.
// Those built-ins are registered on controller-runtime's Prometheus registry
// and are therefore scrape-only: they never reach an OTLP collector. These
// OTel equivalents are what Datadog and Langfuse receive over OTLP. When
// alerting off the Prometheus endpoint prefer the controller_runtime_* series;
// when alerting in Datadog use these.
type instruments struct {
	// Controller reconcile metrics
	reconcileCount    metric.Int64Counter
	reconcileDuration metric.Float64Histogram
	reconcileErrors   metric.Int64Counter
	filesProcessed    metric.Int64Counter

	// MCP tool metrics
	toolCallCount        metric.Int64Counter
	toolCallDuration     metric.Float64Histogram
	toolCallErrors       metric.Int64Counter
	externalCallDuration metric.Float64Histogram
}

// durationBuckets covers the range from fast API calls (5ms) to long reconcile
// loops processing large file batches (up to 5 minutes).
var durationBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300,
}

func newInstruments(mp metric.MeterProvider) (*instruments, error) {
	meter := mp.Meter(meterName)
	var inst instruments
	var err error

	bucketOpt := metric.WithExplicitBucketBoundaries(durationBuckets...)

	// Controller reconcile metrics - recorded by the ReconcileObserver in each reconciler.
	inst.reconcileCount, err = meter.Int64Counter("udc.reconcile.count",
		metric.WithDescription("Total number of reconcile invocations"),
		metric.WithUnit("{reconcile}"))
	if err != nil {
		return nil, err
	}

	inst.reconcileDuration, err = meter.Float64Histogram("udc.reconcile.duration",
		metric.WithDescription("Wall-clock duration of a reconcile loop"),
		metric.WithUnit("s"),
		bucketOpt)
	if err != nil {
		return nil, err
	}

	inst.reconcileErrors, err = meter.Int64Counter("udc.reconcile.errors",
		metric.WithDescription("Total number of reconcile invocations that returned an error"),
		metric.WithUnit("{error}"))
	if err != nil {
		return nil, err
	}

	inst.filesProcessed, err = meter.Int64Counter("udc.files.processed",
		metric.WithDescription("Total number of files processed across all reconcile loops"),
		metric.WithUnit("{file}"))
	if err != nil {
		return nil, err
	}

	// MCP tool metrics - recorded by the ToolObserver in each MCP tool handler.
	inst.toolCallCount, err = meter.Int64Counter("udc.mcp.tool.calls",
		metric.WithDescription("Total number of MCP tool invocations"),
		metric.WithUnit("{call}"))
	if err != nil {
		return nil, err
	}

	inst.toolCallDuration, err = meter.Float64Histogram("udc.mcp.tool.duration",
		metric.WithDescription("Wall-clock duration of an MCP tool invocation"),
		metric.WithUnit("s"),
		bucketOpt)
	if err != nil {
		return nil, err
	}

	inst.toolCallErrors, err = meter.Int64Counter("udc.mcp.tool.errors",
		metric.WithDescription("Total number of MCP tool invocations that returned an error"),
		metric.WithUnit("{error}"))
	if err != nil {
		return nil, err
	}

	// External service call metrics (embedding API, Snowflake, K8s) - recorded by RecordExternalCall.
	inst.externalCallDuration, err = meter.Float64Histogram("udc.mcp.external.duration",
		metric.WithDescription("Duration of external service calls from MCP tools"),
		metric.WithUnit("s"),
		bucketOpt)
	if err != nil {
		return nil, err
	}

	return &inst, nil
}

// ReconcileObserver captures the start time and controller name for a reconcile loop.
//
// Reconcilers do not use this directly: ObserveReconciler wraps the whole
// Reconciler, which keeps telemetry out of the reconcile bodies and lets a
// panicking reconcile be counted. This is the recording primitive behind it.
type ReconcileObserver struct {
	instruments    *instruments
	controllerName string
	start          time.Time
}

// ReconcileObserver returns an observer that tracks reconcile duration and outcome.
// Safe to call on a nil Provider; the returned observer is then a no-op.
func (p *Provider) ReconcileObserver(controllerName string) *ReconcileObserver {
	if p == nil {
		return &ReconcileObserver{}
	}
	return &ReconcileObserver{
		instruments:    p.instruments,
		controllerName: controllerName,
		start:          time.Now(),
	}
}

// End records reconcile count, duration, and error metrics.
// Pass a pointer to the named error return so the deferred call sees the final value.
func (o *ReconcileObserver) End(ctx context.Context, retErr *error) {
	if o == nil || o.instruments == nil {
		return
	}
	attrs := metric.WithAttributes(
		attribute.String("controller", o.controllerName),
	)
	elapsed := time.Since(o.start).Seconds()

	o.instruments.reconcileCount.Add(ctx, 1, attrs)
	o.instruments.reconcileDuration.Record(ctx, elapsed, attrs)
	if retErr != nil && *retErr != nil {
		o.instruments.reconcileErrors.Add(ctx, 1, attrs)
	}
}

// RecordFilesProcessed increments the files-processed counter for a controller.
// Safe to call on a nil Provider.
func (p *Provider) RecordFilesProcessed(ctx context.Context, controllerName string, count int64) {
	if p == nil || p.instruments == nil {
		return
	}
	p.instruments.filesProcessed.Add(ctx, count,
		metric.WithAttributes(attribute.String("controller", controllerName)))
}

// ToolObserver starts a trace span and returns a finish function that records tool metrics.
// The returned context carries the span for propagation to child spans in pkg/ packages.
// Safe to call on a nil Provider; the context is returned unchanged and the
// finish function is a no-op.
//
// The finish function takes the failure cause so the span carries a reason.
// Pass an empty string on success. Callers that only have an error result
// without a machine-readable error should pass the user-facing message.
//
// Tool handlers do not use this directly: the MCP server installs
// tools.TelemetryMiddleware, which calls this once per invocation with the
// registered tool name. This is the recording primitive behind it.
func (p *Provider) ToolObserver(
	ctx context.Context, toolName string, spanAttrs ...attribute.KeyValue,
) (context.Context, func(cause string)) {
	if p == nil || p.instruments == nil {
		return ctx, func(string) {}
	}

	tracer := otel.Tracer(meterName)
	allAttrs := append([]attribute.KeyValue{attribute.String("mcp.tool.name", toolName)}, spanAttrs...)
	ctx, span := tracer.Start(ctx, "mcp.tool/"+toolName,
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(allAttrs...))

	start := time.Now()
	toolAttr := metric.WithAttributes(attribute.String("tool", toolName))

	return ctx, func(cause string) {
		elapsed := time.Since(start).Seconds()

		p.instruments.toolCallCount.Add(ctx, 1, toolAttr)
		p.instruments.toolCallDuration.Record(ctx, elapsed, toolAttr)
		if cause != "" {
			safeCause := sanitize(cause)
			p.instruments.toolCallErrors.Add(ctx, 1, toolAttr)
			span.RecordError(errors.New(safeCause))
			span.SetStatus(codes.Error, safeCause)
		}
		span.End()
	}
}

// callOutcome labels a recorded call so latency can be split by result.
// Bounded to two values, so it does not meaningfully add cardinality.
func callOutcome(err error) string {
	if err != nil {
		return "error"
	}
	return "success"
}

// RecordExternalCall records the duration of an external service call and, on error,
// marks the current span with the error. Used in MCP tool handlers to time
// calls to K8s, Snowflake, and embedding services.
// Safe to call on a nil Provider.
func (p *Provider) RecordExternalCall(
	ctx context.Context, service, operation string, duration time.Duration, err error,
) {
	if p == nil || p.instruments == nil {
		return
	}
	// outcome is what makes this histogram usable for alerting. It is the only
	// external-call instrument - there is no call counter and no error counter
	// - so without it a failing dependency is invisible in metrics, and worse,
	// a dependency that fails fast lowers p99 and leaves a latency SLO green.
	attrs := metric.WithAttributes(
		attribute.String("service", service),
		attribute.String("operation", operation),
		attribute.String("outcome", callOutcome(err)),
	)
	p.instruments.externalCallDuration.Record(ctx, duration.Seconds(), attrs)

	if err == nil {
		return
	}
	// Record an event, but do NOT set the span status.
	//
	// ctx here is the caller's span, not the callee's: the pkg/ functions
	// start and end their own span internally and that context never escapes,
	// so SpanFromContext resolves to the tool span. The callee has already
	// marked its own span via RecordSpanError, and the tool observer sets the
	// tool span's status from the handler's outcome when the call finishes.
	// Setting it here too would make the final status whichever of the three
	// ran last, and would force the tool span to Error even when the handler
	// recovered and returned a successful result.
	//
	// Downstream errors can embed connection strings, SQL text, or tokens, so
	// the message is sanitized before it leaves the process.
	trace.SpanFromContext(ctx).AddEvent("external.call.failed", trace.WithAttributes(
		attribute.String("service", service),
		attribute.String("operation", operation),
		attribute.String("error.message", sanitize(err.Error())),
	))
}
