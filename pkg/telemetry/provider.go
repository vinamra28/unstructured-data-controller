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

// Package telemetry provides OpenTelemetry SDK initialization with dual export:
// OTLP push (for collectors like Datadog/Langfuse) and Prometheus pull (for OSS scraping).
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
)

// cleanupTimeout bounds best-effort shutdown of partially-initialized providers.
// Without it, a hung OTLP exporter could stall Init's error return indefinitely.
const cleanupTimeout = 5 * time.Second

// Config holds configuration for the telemetry provider.
type Config struct {
	// ServiceName identifies this service in telemetry backends (e.g. "unstructured-data-controller").
	ServiceName string

	// ServiceVersion is the version of this service (e.g. "0.1.0").
	ServiceVersion string

	// PrometheusRegisterer is the Prometheus registerer where OTel metrics are published.
	// When non-nil (controller case), custom OTel metrics appear on controller-runtime's
	// existing /metrics endpoint alongside its built-in metrics.
	// When nil (MCP server case), a new registry is created and served via PrometheusHandler().
	PrometheusRegisterer prometheus.Registerer

	// TraceSampleRatio is the fraction of traces exported, between 0 and 1.
	// Zero means "use the SDK default of 1.0". High-throughput callers such as
	// the controller should lower this so continuous reconcile loops do not
	// overwhelm the collector. OTEL_TRACES_SAMPLER_ARG overrides this at runtime.
	TraceSampleRatio float64

	// Logger receives OTel SDK diagnostics: export failures, dropped spans and
	// internal errors. Without it the SDK writes them to a bare log.Print on
	// stderr, which neither binary's structured log pipeline can parse, so an
	// expired collector certificate shows up as flat dashboards with healthy
	// pods and nothing logged. The zero value leaves the SDK default in place.
	Logger logr.Logger
}

// Provider manages OTel MeterProvider and TracerProvider lifecycle.
// It supports dual export: Prometheus (always on) and OTLP gRPC (when OTEL_EXPORTER_OTLP_ENDPOINT is set).
//
// All recording methods are safe to call on a nil Provider, so callers holding
// an optional Provider do not need nil guards at every recording site.
type Provider struct {
	meterProvider  *sdkmetric.MeterProvider
	tracerProvider *sdktrace.TracerProvider
	instruments    *instruments

	// promGatherer is set only when PrometheusRegisterer was nil (MCP server case),
	// so PrometheusHandler() can serve the metrics.
	promGatherer prometheus.Gatherer
}

// Init creates and configures OTel providers with dual export.
// Prometheus export is always active. OTLP push activates when OTEL_EXPORTER_OTLP_ENDPOINT
// is set - the SDK reads that env var automatically, so no manual endpoint parsing is needed.
func Init(ctx context.Context, cfg Config) (_ *Provider, retErr error) {
	// Route SDK diagnostics into the caller's logger before anything can fail,
	// so errors raised during setup are reported the same way as runtime ones.
	// Skipped for a zero-value Logger: the SDK's stderr default is noisy but
	// still better than installing a handler that silently drops errors.
	if cfg.Logger.GetSink() != nil {
		otel.SetLogger(cfg.Logger)
		otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
			cfg.Logger.Error(err, "OpenTelemetry SDK error")
		}))
	}

	// OTEL_SDK_DISABLED is the spec's kill switch for shedding telemetry load.
	// The Go SDK does not act on it, so it has to be handled here. It is read
	// once, at startup: turning telemetry off takes a restart or a rollout,
	// not just a config edit. Instruments are still built, against a no-op
	// meter, so every recording site stays valid and no Prometheus collector
	// or OTLP connection is created.
	if disabled, parseErr := strconv.ParseBool(os.Getenv("OTEL_SDK_DISABLED")); parseErr == nil && disabled {
		inst, instErr := newInstruments(metricnoop.NewMeterProvider())
		if instErr != nil {
			return nil, instErr
		}
		return &Provider{instruments: inst}, nil
	}

	res, err := resource.New(ctx,
		// WithTelemetrySDK adds telemetry.sdk.{name,language,version}, which
		// backends use to attribute data to the emitting SDK.
		resource.WithTelemetrySDK(),
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion(cfg.ServiceVersion),
			// Without service.instance.id every replica of a Deployment emits
			// a byte-identical resource, so a backend cannot tell their series
			// apart or point at the pod that produced a spike.
			semconv.ServiceInstanceID(instanceID()),
		),
		// WithFromEnv reads OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES.
		// Later detectors override earlier ones, so env vars take precedence
		// over the hardcoded defaults above.
		resource.WithFromEnv(),
	)
	if err != nil {
		return nil, err
	}

	p := &Provider{}

	readers, err := p.setupMetricReaders(ctx, cfg)
	if err != nil {
		return nil, err
	}

	mpOpts := []sdkmetric.Option{sdkmetric.WithResource(res)}
	for _, r := range readers {
		mpOpts = append(mpOpts, sdkmetric.WithReader(r))
	}
	p.meterProvider = sdkmetric.NewMeterProvider(mpOpts...)

	// If later steps fail, shut down the MeterProvider to stop its background
	// periodic reader. Cleanup is best effort: its error is joined onto the
	// initialization error rather than replacing it. Cancelled on success.
	meterProviderReady := false
	defer func() {
		if !meterProviderReady {
			if cleanupErr := shutdownBounded(p.meterProvider.Shutdown); cleanupErr != nil {
				retErr = errors.Join(retErr, cleanupErr)
			}
		}
	}()

	// tp is nil when no OTLP endpoint is configured, which leaves the global
	// TracerProvider at its no-op default so span creation costs nothing.
	tp, err := setupTracerProvider(ctx, res, cfg)
	if err != nil {
		return nil, err
	}
	p.tracerProvider = tp

	inst, err := newInstruments(p.meterProvider)
	if err != nil {
		if tp == nil {
			return nil, err
		}
		// Clean up the TracerProvider we just created.
		return nil, errors.Join(err, shutdownBounded(tp.Shutdown))
	}
	p.instruments = inst

	// Set globals only after all components are ready, so a partial failure
	// never leaves the process with half-initialized global providers.
	//
	// Note that this turns on any OTel-aware library in the binary, not just
	// this package: google.golang.org/api wraps its transport in otelhttp,
	// which resolves the global MeterProvider at construction. pkg/gdrive
	// disables that explicitly so the exported metrics stay the udc_* set
	// this package defines.
	otel.SetMeterProvider(p.meterProvider)
	if p.tracerProvider != nil {
		otel.SetTracerProvider(p.tracerProvider)
	}

	meterProviderReady = true
	return p, nil
}

// instanceID identifies this process among replicas of the same service.
// In Kubernetes the hostname is the pod name, which is stable for the pod's
// lifetime and is what an operator correlates a metric spike against. The
// random fallback only applies where no hostname is available, and still
// beats every replica sharing one identity.
func instanceID() string {
	if hostname, err := os.Hostname(); err == nil && hostname != "" {
		return hostname
	}
	return uuid.NewString()
}

// shutdownBounded runs a provider shutdown under cleanupTimeout so a hung
// exporter cannot block the caller indefinitely.
func shutdownBounded(shutdown func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	return shutdown(ctx)
}

// setupMetricReaders creates the Prometheus reader (always) and optional OTLP periodic reader.
func (p *Provider) setupMetricReaders(ctx context.Context, cfg Config) ([]sdkmetric.Reader, error) {
	var readers []sdkmetric.Reader

	promReader, err := p.setupPrometheusReader(cfg)
	if err != nil {
		return nil, err
	}
	readers = append(readers, promReader)

	// OTLP metric exporter activates only when OTEL_EXPORTER_OTLP_ENDPOINT is set.
	// This keeps the default OSS experience zero-config (Prometheus only).
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		otlpExporter, otlpErr := otlpmetricgrpc.New(ctx)
		if otlpErr != nil {
			return nil, otlpErr
		}
		readers = append(readers, sdkmetric.NewPeriodicReader(otlpExporter))
	}

	return readers, nil
}

// setupPrometheusReader creates the OTel Prometheus exporter.
// When a registerer is provided (controller), it registers on the existing Prometheus registry.
// When nil (MCP server), it creates a new registry whose handler is accessible via PrometheusHandler().
func (p *Provider) setupPrometheusReader(cfg Config) (sdkmetric.Reader, error) {
	var opts []promexporter.Option

	if cfg.PrometheusRegisterer != nil {
		opts = append(opts, promexporter.WithRegisterer(cfg.PrometheusRegisterer))
	} else {
		// MCP server case: create a dedicated registry so the metrics endpoint
		// only exposes this application's metrics, not the default process collector.
		reg := prometheus.NewRegistry()
		opts = append(opts, promexporter.WithRegisterer(reg))
		p.promGatherer = reg
	}

	return promexporter.New(opts...)
}

// sdkTraceSamplers are the names the SDK's own OTEL_TRACES_SAMPLER parser
// accepts (vendor/go.opentelemetry.io/otel/sdk/trace/sampler_env.go). The
// value has to be checked against this set rather than merely tested for
// emptiness: on an unrecognised name samplerFromEnv returns an error, the
// provider reports it and then falls back to ParentBased(AlwaysSample()), so
// a typo like "always-off" would silently mean 100% sampling - the opposite
// of what the operator asked for, at the worst possible moment.
var sdkTraceSamplers = map[string]bool{
	"always_on":                true,
	"always_off":               true,
	"traceidratio":             true,
	"parentbased_always_on":    true,
	"parentbased_always_off":   true,
	"parentbased_traceidratio": true,
}

// setupTracerProvider creates a TracerProvider, or returns nil when no OTLP
// endpoint is configured.
//
// Returning nil matters: a TracerProvider with no exporter is NOT free.
// Whether a span records is decided by the sampler alone, so the SDK would
// allocate a full recordingSpan, store every attribute and only discard it at
// End(). Measured at ~325 ns and 528 B per span, on every tool call and every
// instrumented client call. Leaving the API global as its default no-op makes
// span creation genuinely free when telemetry is not being exported.
func setupTracerProvider(
	ctx context.Context, res *resource.Resource, cfg Config,
) (*sdktrace.TracerProvider, error) {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return nil, nil
	}

	traceExporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, err
	}

	tpOpts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExporter),
		// The SDK default AttributeValueLengthLimit is -1, i.e. unlimited.
		// Several attributes are client-controlled - MCP tool arguments reach
		// pipeline.name on a pkg/k8sclient span, for instance - so the bound
		// belongs here rather than at each recording site, where every new
		// attribute would have to remember it.
		sdktrace.WithRawSpanLimits(spanLimits()),
	}

	if sampler := samplerOverride(cfg.TraceSampleRatio); sampler != nil {
		tpOpts = append(tpOpts, sdktrace.WithSampler(sampler))
	}

	return sdktrace.NewTracerProvider(tpOpts...), nil
}

// spanLimits caps attribute values so no single span can carry an unbounded
// payload to the telemetry backend. Other limits keep the SDK defaults.
//
// NewSpanLimits supplies those defaults and WithRawSpanLimits applies the
// struct verbatim, which is what makes an explicit value here meaningful:
// the deprecated WithSpanLimits rewrites any non-positive field back to a
// default, so it cannot express a real bound.
func spanLimits() sdktrace.SpanLimits {
	limits := sdktrace.NewSpanLimits()
	limits.AttributeValueLengthLimit = maxMessageLen
	return limits
}

// samplerOverride returns the sampler this binary should impose, or nil to
// let the SDK read OTEL_TRACES_SAMPLER itself.
//
// WithSampler overrides that variable outright, so it is withheld only when
// the variable names a sampler the SDK actually understands - which is what
// someone setting always_off to protect an overloaded collector expects. The
// membership test matters: on an unrecognised name the SDK's parser errors
// and the provider falls back to ParentBased(AlwaysSample()), so deferring on
// any non-empty value would turn a typo like "always-off" into 100% sampling.
func samplerOverride(ratio float64) sdktrace.Sampler {
	name := strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLER")))
	if sdkTraceSamplers[name] {
		return nil
	}
	return traceSampler(ratio)
}

// traceSampler builds an explicit sampler. The SDK default is AlwaysSample,
// which exports 100% of traces - too much for a controller running continuous
// reconcile loops. Callers set their own ratio via Config.TraceSampleRatio and
// operators override it at runtime with OTEL_TRACES_SAMPLER_ARG.
//
// ParentBased keeps a distributed trace intact: when an upstream service has
// already made a sampling decision, child spans here honour it rather than
// re-rolling and producing partial traces.
func traceSampler(ratio float64) sdktrace.Sampler {
	// Zero is the Go zero value of Config.TraceSampleRatio and means "the
	// caller did not choose", so it takes the SDK's own default of 1.0.
	// Out-of-range values are clamped rather than reset to 1.0: a caller
	// passing a negative ratio is asking for less sampling, and answering
	// that with 100% would be the most expensive possible reading of the
	// request. Operators who want none should set OTEL_TRACES_SAMPLER_ARG=0
	// or OTEL_TRACES_SAMPLER=always_off.
	switch {
	case ratio == 0:
		ratio = 1.0
	case ratio < 0:
		ratio = 0
	case ratio > 1:
		ratio = 1.0
	default:
		// In range and non-zero: use it as given.
	}

	if v := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); v != "" {
		parsed, err := strconv.ParseFloat(v, 64)
		// Positive test, not a negated range: ParseFloat accepts "NaN" with a
		// nil error, and NaN compares false against every relational
		// operator, so `parsed < 0 || parsed > 1` would wave it through and
		// TraceIDRatioBased{NaN} samples nothing.
		if err != nil || !(parsed >= 0 && parsed <= 1) {
			// Logged rather than swallowed: silently ignoring it leaves the
			// operator believing a sampling rate is in force that is not.
			otel.Handle(fmt.Errorf("ignoring invalid OTEL_TRACES_SAMPLER_ARG %q: want a number in [0,1]", v))
		} else {
			ratio = parsed
		}
	}

	return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
}

// PrometheusHandler returns an http.Handler serving Prometheus metrics.
// Returns nil when the provider was initialized with an external registerer
// (controller case, where metrics are served by controller-runtime's built-in endpoint).
func (p *Provider) PrometheusHandler() http.Handler {
	if p == nil || p.promGatherer == nil {
		return nil
	}
	return promhttp.HandlerFor(p.promGatherer, promhttp.HandlerOpts{})
}

// Shutdown flushes pending telemetry and releases resources.
// Safe to call multiple times.
//
// Call this after manager.Start returns rather than registering the Provider
// as a manager.Runnable. A Runnable reporting NeedLeaderElection()==false
// goes into controller-runtime's `Others` group, which the manager stops
// BEFORE the LeaderElection group holding the reconcilers - so telemetry
// would be torn down while every controller was still draining, losing
// exactly the reconciles worth observing. Budget the caller's shutdown so
// this still runs inside the pod's termination grace period.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p == nil {
		return nil
	}
	var errs []error
	if p.meterProvider != nil {
		if err := p.meterProvider.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if p.tracerProvider != nil {
		if err := p.tracerProvider.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
