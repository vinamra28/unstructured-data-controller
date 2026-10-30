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

package tools

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	udctelemetry "github.com/redhat-data-and-ai/unstructured-data-controller/pkg/telemetry"
)

// setupTestProvider builds a real Provider exporting to a registry the test
// owns, so assertions run against the same path production uses.
func setupTestProvider(t *testing.T) (*udctelemetry.Provider, *prometheus.Registry) {
	t.Helper()

	// The tool allow-list is populated by registration, so every test that
	// records under a real tool name needs it populated first. Doing it here
	// keeps the tests order-independent under -shuffle.
	withRegisteredTools(t)

	// Keep the OTLP exporter out of the test: it would try to dial a collector.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	registry := prometheus.NewRegistry()
	provider, err := udctelemetry.Init(context.Background(), udctelemetry.Config{
		ServiceName:          "tools-test",
		ServiceVersion:       "0.0.0",
		PrometheusRegisterer: registry,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, provider.Shutdown(context.Background()))
	})

	return provider, registry
}

// counterValue returns the value of the counter whose family name contains
// nameFragment and whose labels include tool=toolName. Returns -1 when the
// series was never recorded, which is distinct from a recorded zero.
func counterValue(t *testing.T, registry *prometheus.Registry, nameFragment, toolName string) float64 {
	t.Helper()

	families, err := registry.Gather()
	require.NoError(t, err)

	for _, family := range families {
		if !strings.Contains(family.GetName(), nameFragment) {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "tool" && label.GetValue() == toolName {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	return -1
}

// withRegisteredTools populates the allow-list the way main.go does, so tests
// that depend on it do not depend on another test having run first.
func withRegisteredTools(t *testing.T) {
	t.Helper()
	server := mcp.NewServer(&mcp.Implementation{Name: "test", Version: "0"}, nil)
	RegisterListPipelines(server, nil, nil)
	RegisterGetChunksForEmbeddings(server, nil, nil, nil)
	RegisterGetProcessedDocument(server, nil, nil)
}

func toolCallRequest(toolName string) *mcp.CallToolRequest {
	return &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Name: toolName}}
}

// TestTelemetryMiddlewareRecordsRegisteredToolName is the regression test for
// a handler-side observer being given a label that drifted from the name the
// tool was actually registered under.
func TestTelemetryMiddlewareRecordsRegisteredToolName(t *testing.T) {
	provider, registry := setupTestProvider(t)

	handler := func(context.Context, string, mcp.Request) (mcp.Result, error) {
		return &mcp.CallToolResult{}, nil
	}
	wrapped := TelemetryMiddleware(provider)(handler)

	const toolName = "list_unstructured_data_pipelines_for_user"
	_, err := wrapped(context.Background(), methodCallTool, toolCallRequest(toolName))
	require.NoError(t, err)

	assert.Equal(t, float64(1), counterValue(t, registry, "tool_calls", toolName))
	assert.Equal(t, float64(-1), counterValue(t, registry, "tool_errors", toolName),
		"error counter should not be recorded for a successful call")
}

// TestTelemetryMiddlewareBoundsUnknownToolNames is the cardinality guard. The
// SDK rejects an unknown tool inside callTool, which runs after this
// middleware, so an authenticated client could otherwise mint a fresh
// Prometheus series and span name per request just by varying the string.
func TestTelemetryMiddlewareBoundsUnknownToolNames(t *testing.T) {
	provider, registry := setupTestProvider(t)

	handler := func(context.Context, string, mcp.Request) (mcp.Result, error) {
		return nil, errors.New(`unknown tool "nope"`)
	}
	wrapped := TelemetryMiddleware(provider)(handler)

	// Distinct attacker-chosen names must collapse onto one series.
	for _, name := range []string{"attacker-1", "attacker-2", "attacker-3"} {
		_, err := wrapped(context.Background(), methodCallTool, toolCallRequest(name))
		require.Error(t, err)
		assert.Equal(t, float64(-1), counterValue(t, registry, "tool_calls", name),
			"an unregistered name must never become a label value")
	}

	assert.Equal(t, float64(3), counterValue(t, registry, "tool_calls", toolNameUnknown),
		"all unregistered names should collapse onto the bounded label")
}

func TestToolNameAllowList(t *testing.T) {
	withRegisteredTools(t)

	// Registered names pass through unchanged so dashboards stay meaningful.
	for _, name := range []string{ToolListPipelines, ToolGetChunks, ToolGetProcessedDocument} {
		assert.Equal(t, name, toolName(name))
	}

	for _, name := range []string{"", "nope", "List_Unstructured_Data_Pipelines_For_User", strings.Repeat("x", 500)} {
		assert.Equal(t, toolNameUnknown, toolName(name),
			"unregistered name %q must be bounded", name)
	}
}

// Registration is what populates the allow-list, so this asserts the real
// set rather than a hardcoded count: every name the Register* functions put
// on a server must come back bounded to itself, and nothing else may.
func TestRegisteredToolsComesFromRegistration(t *testing.T) {
	withRegisteredTools(t)

	registeredToolsMu.RLock()
	got := make([]string, 0, len(registeredTools))
	for name := range registeredTools {
		got = append(got, name)
	}
	registeredToolsMu.RUnlock()

	// Subset, not exact: registeredTools is a package global that other tests
	// also populate and nothing resets. What matters is that registration put
	// each name in, not that no other test added one.
	assert.Subset(t, got,
		[]string{ToolListPipelines, ToolGetChunks, ToolGetProcessedDocument},
		"registering a tool must record its own name")

	for _, name := range got {
		assert.Equal(t, name, toolName(name))
	}
}

func TestTelemetryMiddlewareRecordsErrorResult(t *testing.T) {
	provider, registry := setupTestProvider(t)

	handler := func(context.Context, string, mcp.Request) (mcp.Result, error) {
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{&mcp.TextContent{Text: "Error listing pipelines: boom"}},
		}, nil
	}
	wrapped := TelemetryMiddleware(provider)(handler)

	const toolName = "get_chunks_for_embeddings"
	res, err := wrapped(context.Background(), methodCallTool, toolCallRequest(toolName))

	// A tool failure is reported in the result, not as a transport error, so
	// the middleware must pass both through unchanged.
	require.NoError(t, err)
	require.NotNil(t, res)

	assert.Equal(t, float64(1), counterValue(t, registry, "tool_calls", toolName))
	assert.Equal(t, float64(1), counterValue(t, registry, "tool_errors", toolName))
}

func TestTelemetryMiddlewareRecordsTransportError(t *testing.T) {
	provider, registry := setupTestProvider(t)

	handlerErr := errors.New("session closed")
	handler := func(context.Context, string, mcp.Request) (mcp.Result, error) {
		return nil, handlerErr
	}
	wrapped := TelemetryMiddleware(provider)(handler)

	const toolName = "get_processed_document"
	_, err := wrapped(context.Background(), methodCallTool, toolCallRequest(toolName))
	require.ErrorIs(t, err, handlerErr)

	assert.Equal(t, float64(1), counterValue(t, registry, "tool_errors", toolName))
}

// TestTelemetryMiddlewareRecordsPanic covers what a handler-side defer cannot:
// a panicking handler must still be counted as a failure, not silently
// omitted or recorded as a success.
func TestTelemetryMiddlewareRecordsPanic(t *testing.T) {
	provider, registry := setupTestProvider(t)

	handler := func(context.Context, string, mcp.Request) (mcp.Result, error) {
		panic("boom")
	}
	wrapped := TelemetryMiddleware(provider)(handler)

	const toolName = "get_chunks_for_embeddings"
	require.PanicsWithValue(t, "boom", func() {
		_, _ = wrapped(context.Background(), methodCallTool, toolCallRequest(toolName))
	})

	assert.Equal(t, float64(1), counterValue(t, registry, "tool_calls", toolName))
	assert.Equal(t, float64(1), counterValue(t, registry, "tool_errors", toolName))
}

func TestTelemetryMiddlewareIgnoresNonToolMethods(t *testing.T) {
	provider, registry := setupTestProvider(t)

	called := false
	handler := func(context.Context, string, mcp.Request) (mcp.Result, error) {
		called = true
		return &mcp.ListToolsResult{}, nil
	}
	wrapped := TelemetryMiddleware(provider)(handler)

	_, err := wrapped(context.Background(), "tools/list", &mcp.ListToolsRequest{})
	require.NoError(t, err)
	assert.True(t, called, "non-tool methods must still reach the handler")

	families, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		assert.NotContains(t, family.GetName(), "udc_mcp_tool_calls",
			"tools/list must not be counted as a tool call")
	}
}

func TestTelemetryMiddlewareNilProviderIsSafe(t *testing.T) {
	handler := func(context.Context, string, mcp.Request) (mcp.Result, error) {
		return &mcp.CallToolResult{}, nil
	}
	wrapped := TelemetryMiddleware(nil)(handler)

	require.NotPanics(t, func() {
		res, err := wrapped(context.Background(), methodCallTool, toolCallRequest("any_tool"))
		require.NoError(t, err)
		require.NotNil(t, res)
	})
}

func TestToolCallCause(t *testing.T) {
	tests := []struct {
		name     string
		res      mcp.Result
		err      error
		expected string
	}{
		{
			name:     "successful result has no cause",
			res:      &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "ok"}}},
			expected: "",
		},
		{
			name:     "transport error takes precedence over the result",
			res:      &mcp.CallToolResult{},
			err:      errors.New("session closed"),
			expected: "session closed",
		},
		{
			name: "error result reports its text content",
			res: &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{&mcp.TextContent{Text: "Error querying Snowflake: timeout"}},
			},
			expected: "Error querying Snowflake: timeout",
		},
		{
			name:     "error result without content falls back to a generic cause",
			res:      &mcp.CallToolResult{IsError: true},
			expected: "tool call failed",
		},
		{
			name: "error result skips empty text content",
			res: &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{&mcp.TextContent{}, &mcp.TextContent{Text: "real cause"}},
			},
			expected: "real cause",
		},
		{
			// callTool returns a nil result alongside a nil error for an
			// unknown tool only after the SDK has already rejected it, so this
			// is a protocol violation rather than a failure to attribute.
			name:     "nil result without an error is not a failure",
			res:      (*mcp.CallToolResult)(nil),
			expected: "",
		},
		{
			name:     "non-tool result is not a failure",
			res:      &mcp.ListToolsResult{},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, toolCallCause(tt.res, tt.err))
		})
	}
}
