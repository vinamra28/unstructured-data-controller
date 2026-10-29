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
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
)

// preserveGlobals restores the process-wide OTel providers after a test.
// Init replaces them, so without this a shut-down provider would leak into
// subsequent tests in the package.
func preserveGlobals(t *testing.T) {
	t.Helper()
	origMeterProvider := otel.GetMeterProvider()
	origTracerProvider := otel.GetTracerProvider()
	t.Cleanup(func() {
		otel.SetMeterProvider(origMeterProvider)
		otel.SetTracerProvider(origTracerProvider)
	})
}

// targetInfoLabels returns the labels of the Prometheus target_info series,
// which the OTel exporter builds from the resource. It is the only place the
// resource surfaces without reaching into unexported Provider state.
func targetInfoLabels(t *testing.T, registry *prometheus.Registry) map[string]string {
	t.Helper()

	families, err := registry.Gather()
	require.NoError(t, err)

	for _, family := range families {
		if family.GetName() != "target_info" {
			continue
		}
		require.NotEmpty(t, family.GetMetric())
		labels := map[string]string{}
		for _, label := range family.GetMetric()[0].GetLabel() {
			labels[label.GetName()] = label.GetValue()
		}
		return labels
	}
	t.Fatal("target_info series not found; the exporter should publish the resource")
	return nil
}

func TestInitResourceIdentifiesTheSDKAndInstance(t *testing.T) {
	preserveGlobals(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_SDK_DISABLED", "")
	registry := prometheus.NewRegistry()

	provider, err := Init(context.Background(), Config{
		ServiceName:          "test-service",
		ServiceVersion:       "0.0.1",
		PrometheusRegisterer: registry,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, provider.Shutdown(context.Background())) }()

	labels := targetInfoLabels(t, registry)

	// Without service.instance.id every replica emits an identical resource
	// and a backend cannot tell which pod produced a series.
	assert.NotEmpty(t, labels["service_instance_id"],
		"resource should carry service.instance.id")
	assert.Equal(t, "opentelemetry", labels["telemetry_sdk_name"],
		"resource should carry telemetry.sdk.* from WithTelemetrySDK")
	assert.Equal(t, "go", labels["telemetry_sdk_language"])
	assert.Equal(t, "test-service", labels["service_name"])
}

func TestInstanceIDIsStableWithinAProcess(t *testing.T) {
	// The pod name is what an operator correlates against, so it must not
	// change between calls within one process.
	assert.Equal(t, instanceID(), instanceID())
	assert.NotEmpty(t, instanceID())
}

func TestInitHonorsSDKDisabled(t *testing.T) {
	preserveGlobals(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_SDK_DISABLED", "true")
	registry := prometheus.NewRegistry()

	provider, err := Init(context.Background(), Config{
		ServiceName:          "test-service",
		ServiceVersion:       "0.0.1",
		PrometheusRegisterer: registry,
	})
	require.NoError(t, err)

	assert.Nil(t, provider.meterProvider, "no MeterProvider should be built when the SDK is disabled")
	assert.Nil(t, provider.tracerProvider, "no TracerProvider should be built when the SDK is disabled")

	families, err := registry.Gather()
	require.NoError(t, err)
	assert.Empty(t, families, "a disabled SDK must not publish anything to the registry")

	// Recording sites must stay valid: the kill switch is for load, not for
	// making every caller nil-check.
	assert.NotPanics(t, func() {
		provider.RecordFilesProcessed(context.Background(), "AnyController", 1)
		provider.RecordExternalCall(context.Background(), "snowflake", "query", time.Second, nil)
		var retErr error
		provider.ReconcileObserver("AnyController").End(context.Background(), &retErr)
		_, endTool := provider.ToolObserver(context.Background(), "any_tool")
		endTool("")
		require.NoError(t, provider.Shutdown(context.Background()))
	})
}

func TestInitIgnoresMalformedSDKDisabled(t *testing.T) {
	preserveGlobals(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	// A value that is not a bool must not silently disable telemetry; the
	// safe reading of a typo is "the operator did not mean to turn this off".
	t.Setenv("OTEL_SDK_DISABLED", "yes-please")

	provider, err := Init(context.Background(), Config{
		ServiceName:    "test-service",
		ServiceVersion: "0.0.1",
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, provider.Shutdown(context.Background())) }()

	assert.NotNil(t, provider.meterProvider, "malformed OTEL_SDK_DISABLED should leave the SDK enabled")
}

func TestInitDefersToOTELTracesSampler(t *testing.T) {
	preserveGlobals(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_SDK_DISABLED", "")
	// always_off is what an operator reaches for to protect an overloaded
	// collector. WithSampler would override it outright, so Init must not
	// apply its own sampler when this is set.
	t.Setenv("OTEL_TRACES_SAMPLER", "always_off")

	provider, err := Init(context.Background(), Config{
		ServiceName:    "test-service",
		ServiceVersion: "0.0.1",
		// Would otherwise force 100% sampling and mask the env var.
		TraceSampleRatio: 1.0,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, provider.Shutdown(context.Background())) }()

	_, span := provider.tracerProvider.Tracer("test").Start(context.Background(), "op")
	defer span.End()
	assert.False(t, span.IsRecording(), "OTEL_TRACES_SAMPLER=always_off should win over TraceSampleRatio")
}

func TestInitAppliesConfiguredSamplerWithoutTheEnvVar(t *testing.T) {
	preserveGlobals(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_SDK_DISABLED", "")
	t.Setenv("OTEL_TRACES_SAMPLER", "")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "")

	provider, err := Init(context.Background(), Config{
		ServiceName:      "test-service",
		ServiceVersion:   "0.0.1",
		TraceSampleRatio: 1.0,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, provider.Shutdown(context.Background())) }()

	_, span := provider.tracerProvider.Tracer("test").Start(context.Background(), "op")
	defer span.End()
	assert.True(t, span.IsRecording(), "the configured ratio should apply when no sampler env var is set")
}

func TestInitWithDefaults(t *testing.T) {
	preserveGlobals(t)
	// Without OTEL_EXPORTER_OTLP_ENDPOINT, only Prometheus export should be active.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	p, err := Init(context.Background(), Config{
		ServiceName:    "test-service",
		ServiceVersion: "0.0.1",
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, p.Shutdown(context.Background())) }()

	assert.NotNil(t, p.meterProvider, "MeterProvider should be initialized")
	assert.NotNil(t, p.tracerProvider, "TracerProvider should be initialized")
	assert.NotNil(t, p.instruments, "instruments should be initialized")
}

func TestInitWithExternalRegisterer(t *testing.T) {
	preserveGlobals(t)
	// Simulates the controller case: pass an existing Prometheus registry.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	reg := prometheus.NewRegistry()

	p, err := Init(context.Background(), Config{
		ServiceName:          "test-controller",
		ServiceVersion:       "0.0.1",
		PrometheusRegisterer: reg,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, p.Shutdown(context.Background())) }()

	// When an external registerer is provided, PrometheusHandler() returns nil
	// because the controller-runtime endpoint already serves the metrics.
	assert.Nil(t, p.PrometheusHandler(), "PrometheusHandler should be nil when using an external registerer")
}

func TestPrometheusHandlerServesMetrics(t *testing.T) {
	preserveGlobals(t)
	// Simulates the MCP server case: no external registerer, so we create our own.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	p, err := Init(context.Background(), Config{
		ServiceName:    "test-mcp-server",
		ServiceVersion: "0.0.1",
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, p.Shutdown(context.Background())) }()

	handler := p.PrometheusHandler()
	require.NotNil(t, handler, "PrometheusHandler should be non-nil when no external registerer is provided")

	// Record a metric so the /metrics endpoint has something to serve.
	p.RecordFilesProcessed(context.Background(), "test-controller", 5)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	assert.Equal(t, http.StatusOK, rec.Code)

	body, err := io.ReadAll(rec.Body)
	require.NoError(t, err)
	bodyStr := string(body)

	// The OTel Prometheus exporter translates OTel metric names to Prometheus conventions:
	// "udc.files.processed" -> "udc_files_processed" (dots to underscores, counter suffix added).
	assert.True(t, strings.Contains(bodyStr, "udc_files_processed"),
		"expected udc_files_processed in metrics output, got: %s", bodyStr)
}

func TestShutdownIsIdempotent(t *testing.T) {
	preserveGlobals(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	p, err := Init(context.Background(), Config{
		ServiceName:    "test-service",
		ServiceVersion: "0.0.1",
	})
	require.NoError(t, err)

	require.NoError(t, p.Shutdown(context.Background()))
	// Second shutdown should not panic or return an unexpected error.
	// The OTel SDK may return an error on double shutdown, but it should not panic.
	_ = p.Shutdown(context.Background())
}

func TestStartBlocksUntilContextCancelled(t *testing.T) {
	preserveGlobals(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	p, err := Init(context.Background(), Config{
		ServiceName:    "test-service",
		ServiceVersion: "0.0.1",
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- p.Start(ctx)
	}()

	// Start should be blocking.
	select {
	case <-done:
		t.Fatal("Start returned before context was cancelled")
	case <-time.After(50 * time.Millisecond):
		// Expected: Start is still blocking.
	}

	cancel()

	select {
	case err := <-done:
		// Start should return after context cancellation. The SDK may return
		// "already shut down" if Shutdown was already called, which is acceptable.
		if err != nil {
			t.Logf("Start returned error after cancellation (acceptable): %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after context cancellation")
	}
}
