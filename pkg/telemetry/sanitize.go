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
	"errors"
	"regexp"
	"unicode/utf8"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// maxMessageLen caps sanitized messages. Snowflake and HTTP clients can return
// multi-kilobyte errors containing full query text or response bodies; span
// attributes that large are expensive to export and rarely add diagnostic value
// beyond the first line.
const maxMessageLen = 256

const redacted = "[REDACTED]"

// secretRules match credential material that downstream clients commonly
// embed in error strings. Errors recorded on spans leave the process and land
// in Datadog and Langfuse, so these are stripped before export.
//
// Each rule carries its own replacement template. A single shared template
// cannot work: what is safe to keep differs per pattern (the URL scheme, the
// auth scheme word, the key name), and keeping it is what leaves the message
// diagnosable. The templates are compile-time constants, so the $-expansion
// ReplaceAllString performs can never be driven by the error text itself.
var secretRules = []struct {
	pattern     *regexp.Regexp
	replacement string
}{
	{
		// Credentials in a URL userinfo section: scheme://user:password@host
		//
		// Both userinfo parts use [^/\s]* rather than excluding '@'. A password
		// may itself contain '@', and stopping at the first one would leave the
		// remainder of the password in the message. Greedy matching consumes
		// through the last '@' of the userinfo; '/' and whitespace still bound
		// the match so a later unrelated '@' (an email address, say) is safe.
		//
		// The scheme is kept so the host context survives.
		pattern:     regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://)[^/\s]*:[^/\s]*@`),
		replacement: "${1}" + redacted + "@",
	},
	{
		// Authorization header values: "Bearer abc123", "Basic dXNlcjpwYXNz".
		// The scheme word is kept: it says which auth scheme was rejected and
		// is not itself secret.
		pattern:     regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9\-._~+/=]+`),
		replacement: "${1} " + redacted,
	},
	{
		// key=value and key: value pairs for sensitive key names. The optional
		// quote after the key name covers JSON bodies ("api_key": "...") as well
		// as bare connection-string and query-parameter forms (password=...).
		//
		// The [\w.-]* prefix is what catches the compound names that actually
		// appear in OAuth, SSO and AWS error bodies: access_token,
		// client_secret, refresh_token, aws_secret_access_key, x-api-key. A
		// plain \b before the alternation cannot match any of them, because
		// '_' is a word character so there is no boundary before "token" in
		// "access_token". The prefix is inside group 1 so the full key name
		// survives redaction.
		//
		// The key name and separator are kept so a reader can tell which
		// credential the message was carrying; only the value is replaced.
		pattern: regexp.MustCompile(
			`(?i)\b([\w.-]*(?:password|passwd|token|secret|api[_-]?key|access[_-]?key|private[_-]?key|credential))` +
				`("?\s*[=:]\s*)("[^"]*"|'[^']*'|[^\s,;&)}]+)`),
		replacement: "${1}${2}" + redacted,
	},
}

// sanitize strips credential material from a message and truncates it so error
// text recorded on spans cannot leak secrets to telemetry backends.
func sanitize(msg string) string {
	for _, rule := range secretRules {
		msg = rule.pattern.ReplaceAllString(msg, rule.replacement)
	}

	if len(msg) > maxMessageLen {
		// Back off to a rune boundary. Cutting mid-rune yields invalid UTF-8,
		// which protobuf rejects for proto3 string fields; the OTLP exporter
		// then fails the whole export and the batch processor drops every span
		// in the batch, not just the one carrying this message. Crawled Drive
		// and Git content makes non-ASCII error text routine. At most three
		// continuation bytes are skipped.
		limit := maxMessageLen
		for limit > 0 && !utf8.RuneStart(msg[limit]) {
			limit--
		}
		return msg[:limit] + "...[truncated]"
	}
	return msg
}

// RecordSpanError marks span as failed with a sanitized form of err.
//
// Use this instead of calling span.RecordError directly. Errors from
// downstream clients routinely embed connection strings, query text, or HTTP
// response bodies, and span attributes are exported to Datadog and Langfuse.
// Passing a nil err is a no-op so callers can defer this unconditionally.
func RecordSpanError(span trace.Span, err error) {
	if err == nil {
		return
	}
	safeMsg := sanitize(err.Error())
	span.RecordError(errors.New(safeMsg))
	span.SetStatus(codes.Error, safeMsg)
}
