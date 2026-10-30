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
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"
	udctelemetry "github.com/redhat-data-and-ai/unstructured-data-controller/pkg/telemetry"
)

// methodCallTool is the JSON-RPC method name for a tool invocation. The MCP
// SDK keeps its own copy of this constant unexported.
const methodCallTool = "tools/call"

// Tool names, shared between registration and telemetry so the two cannot
// drift. Adding a tool means adding its name here and to registeredTools.
const (
	ToolListPipelines        = "list_unstructured_data_pipelines_for_user"
	ToolGetChunks            = "get_chunks_for_embeddings"
	ToolGetProcessedDocument = "get_processed_document"
)

// toolNameUnknown bounds the tool label and span name for calls naming a tool
// that was never registered.
//
// The SDK rejects an unknown tool inside callTool, which runs *after*
// receiving middleware, so by the time the request is refused this middleware
// has already seen the name. Any authenticated client could otherwise mint a
// new Prometheus series and a new span name per request just by varying the
// string, growing the registry without bound. The angle brackets cannot
// collide with a real tool name.
const toolNameUnknown = "<unknown>"

// registeredTools is the allow-list behind toolName. The SDK exposes no way
// to enumerate what AddTool registered, so registerTool records each name as
// it goes in. Deriving the set from registration rather than restating it by
// hand is what keeps the two from drifting: a tool added through registerTool
// is bounded correctly with no second edit, and one added through mcp.AddTool
// directly reports under toolNameUnknown, which is visible rather than
// dangerous.
//
// Guarded because registration happens at startup while reads happen per
// request; the lock is uncontended in practice.
var (
	registeredToolsMu sync.RWMutex
	registeredTools   = map[string]bool{}
)

// registerTool adds a tool to the server and records its name for telemetry.
// Use this in place of mcp.AddTool.
func registerTool[In, Out any](s *mcp.Server, tool *mcp.Tool, handler mcp.ToolHandlerFor[In, Out]) {
	registeredToolsMu.Lock()
	registeredTools[tool.Name] = true
	registeredToolsMu.Unlock()

	mcp.AddTool(s, tool, handler)
}

// toolName maps an incoming tool name onto the bounded label set.
func toolName(requested string) string {
	registeredToolsMu.RLock()
	defer registeredToolsMu.RUnlock()

	if registeredTools[requested] {
		return requested
	}
	return toolNameUnknown
}

// causeToolPanicked is recorded when a handler unwinds from a panic. The panic
// value is deliberately not used: it can embed arbitrary downstream data, and
// the span message is exported to Datadog and Langfuse.
const causeToolPanicked = "tool call panicked"

// TelemetryMiddleware returns MCP receiving middleware that records call
// count, duration, and errors for every tool invocation, and wraps each call
// in a trace span.
//
// Instrumenting here rather than inside each handler means the recorded tool
// name is always the registered name, newly added tools are covered without
// touching them, and a handler that panics is still counted.
//
// Safe to use with a nil Provider; the observer is then a no-op.
func TelemetryMiddleware(metricsProvider *udctelemetry.Provider) mcp.Middleware {
	return func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			// Only tool calls carry these metrics; initialize, tools/list,
			// ping and the rest pass straight through.
			callReq, isToolCall := req.(*mcp.CallToolRequest)
			if method != methodCallTool || !isToolCall {
				return next(ctx, method, req)
			}

			ctx, endTool := metricsProvider.ToolObserver(ctx, toolName(callReq.Params.Name))

			// cause is resolved from the outcome on the way out. A panic skips
			// that assignment, so it starts at the panic reason and is
			// overwritten only once the handler has actually returned.
			cause := causeToolPanicked
			defer func() { endTool(cause) }()

			res, err := next(ctx, method, req)
			cause = toolCallCause(res, err)
			return res, err
		}
	}
}

// dependencyError filters out failures that are really normal outcomes, so
// they do not inflate the error share of udc.mcp.external.duration. That
// histogram is the only health signal for Snowflake, the embedding API and
// K8s, and an alert on it has to distinguish "the dependency is down" from
// "the caller asked for something that does not exist".
func dependencyError(err error) error {
	if errors.Is(err, snowflake.ErrDocumentNotFound) {
		return nil
	}
	return err
}

// setToolSpanAttributes records client-supplied tool arguments on the span the
// middleware started.
//
// Values are truncated and scrubbed rather than passed through: they come
// verbatim from the JSON tool arguments, and a file path can carry a token or
// personal data. This is the same reasoning that bounds the tool name above
// and keeps SQL text off the pkg/snowflake spans - spans leave the process.
// Empty values are dropped so the span is not littered with blanks.
func setToolSpanAttributes(ctx context.Context, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	bounded := make([]attribute.KeyValue, 0, len(attrs))
	for _, attr := range attrs {
		value := attr.Value.AsString()
		if value == "" {
			continue
		}
		bounded = append(bounded, attr.Key.String(udctelemetry.SafeAttributeValue(value)))
	}
	span.SetAttributes(bounded...)
}

// toolCallCause extracts the failure reason from a tool call outcome so the
// span carries why a call failed, not just that it did. Returns an empty
// string for success, which the observer treats as no error.
//
// Both return values have to be inspected: a protocol-level failure surfaces
// as a non-nil error, while a handler reports a user-facing failure through
// CallToolResult.IsError with a nil error.
func toolCallCause(res mcp.Result, err error) string {
	if err != nil {
		return err.Error()
	}
	callRes, ok := res.(*mcp.CallToolResult)
	if !ok || callRes == nil || !callRes.IsError {
		return ""
	}
	// SetError carries the cause as a Go error. No handler here uses it yet -
	// they all build CallToolResult literals - so this is defensive, but it
	// is the precise reason when present and beats scraping the rendered
	// text, which SetError only fills in when Content was empty.
	if resErr := callRes.GetError(); resErr != nil {
		return resErr.Error()
	}
	for _, content := range callRes.Content {
		if text, isText := content.(*mcp.TextContent); isText && text.Text != "" {
			return text.Text
		}
	}
	return "tool call failed"
}
