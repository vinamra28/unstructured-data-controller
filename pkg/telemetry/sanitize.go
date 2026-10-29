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

import "regexp"

// maxMessageLen caps sanitized messages. Snowflake and HTTP clients can return
// multi-kilobyte errors containing full query text or response bodies; span
// attributes that large are expensive to export and rarely add diagnostic value
// beyond the first line.
const maxMessageLen = 256

const redacted = "[REDACTED]"

// secretPatterns match credential material that downstream clients commonly
// embed in error strings. Errors recorded on spans leave the process and land
// in Datadog and Langfuse, so these are stripped before export.
var secretPatterns = []*regexp.Regexp{
	// Credentials in a URL userinfo section: scheme://user:password@host
	regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://)[^/\s:@]+:[^/\s@]+@`),
	// Authorization header values: "Bearer abc123", "Basic dXNlcjpwYXNz"
	regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9\-._~+/=]+`),
	// key=value and key: value pairs for sensitive key names. The optional
	// quote after the key name covers JSON bodies ("api_key": "...") as well
	// as bare connection-string and query-parameter forms (password=...).
	regexp.MustCompile(
		`(?i)\b(password|passwd|token|secret|api[_-]?key|access[_-]?key|private[_-]?key|credential)` +
			`"?\s*[=:]\s*("[^"]*"|'[^']*'|[^\s,;&)}]+)`),
}

// sanitize strips credential material from a message and truncates it so error
// text recorded on spans cannot leak secrets to telemetry backends.
func sanitize(msg string) string {
	for _, pattern := range secretPatterns {
		msg = pattern.ReplaceAllStringFunc(msg, func(match string) string {
			// Preserve the leading scheme for URL matches so the host context
			// survives; everything after it is credential material.
			if loc := pattern.FindStringSubmatchIndex(match); len(loc) >= 4 && loc[2] == 0 {
				return match[loc[2]:loc[3]] + redacted + "@"
			}
			return redacted
		})
	}

	if len(msg) > maxMessageLen {
		return msg[:maxMessageLen] + "...[truncated]"
	}
	return msg
}
