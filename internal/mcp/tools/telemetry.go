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

	"github.com/modelcontextprotocol/go-sdk/mcp"

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

// registeredTools is the allow-list behind toolName. The SDK exposes no way to
// enumerate what AddTool registered, so the set is maintained here.
var registeredTools = map[string]bool{
	ToolListPipelines:        true,
	ToolGetChunks:            true,
	ToolGetProcessedDocument: true,
}

// toolName maps an incoming tool name onto the bounded label set.
func toolName(requested string) string {
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
	for _, content := range callRes.Content {
		if text, isText := content.(*mcp.TextContent); isText && text.Text != "" {
			return text.Text
		}
	}
	return "tool call failed"
}
