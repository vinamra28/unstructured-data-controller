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

import "github.com/modelcontextprotocol/go-sdk/mcp"

// toolFailureCause extracts the failure reason from a tool result so the
// telemetry span carries why a call failed, not just that it did.
// Returns an empty string for successful results, which the observer treats
// as success. Tool handlers signal failure through CallToolResult.IsError
// rather than a Go error, so the reason lives in the result's text content.
func toolFailureCause(res *mcp.CallToolResult) string {
	if res == nil || !res.IsError {
		return ""
	}
	for _, content := range res.Content {
		if text, ok := content.(*mcp.TextContent); ok && text.Text != "" {
			return text.Text
		}
	}
	return "tool call failed"
}
