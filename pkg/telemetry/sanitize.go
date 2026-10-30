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
	"strings"
	"unicode/utf8"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// maxMessageLen caps sanitized messages. Snowflake and HTTP clients can return
// multi-kilobyte errors containing full query text or response bodies; span
// attributes that large are expensive to export and rarely add diagnostic value
// beyond the first line.
const maxMessageLen = 256

// maxSanitizeInput caps how much text the redaction rules will scan. Well
// above maxMessageLen so that a credential near the start still has its
// terminator in range, and far enough below "unbounded" that a hostile input
// cannot turn three regex passes into a denial of service.
const maxSanitizeInput = 8 << 10

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
		// The scheme is REQUIRED. Making it optional turns this into "any
		// whitespace-free word:word@ run" and destroys ordinary identifiers:
		// an image ref (quay.io/org/app:v1@sha256:...), a pod reference
		// (default:web-0@node-1) and a compact JSON body containing a service
		// account address all match. Scheme-less DSNs are handled by the next
		// rule, which is narrow enough to tell them apart.
		//
		// Both userinfo halves are deliberately permissive ([^/\s]*). Nothing
		// constrains what a Secret holds, so excluding quotes, commas or
		// braces to tidy up a match would simply stop redacting any password
		// containing one - failing open on exactly the generated passwords
		// most likely to contain them. '@' is allowed because a password may
		// contain one; greedy matching runs to the last '@' of the userinfo,
		// and '/' and whitespace still bound the match so a bare email
		// address later in the message is untouched.
		//
		// The scheme is kept so the host context survives.
		pattern:     regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://)[^/\s]*:[^/\s]*@`),
		replacement: "${1}" + redacted + "@",
	},
	{
		// Scheme-less DSN userinfo: user:password@account/database/schema,
		// which is the form Snowflake connection strings take.
		//
		// Kept narrow precisely because there is no scheme to anchor on: the
		// password may not contain '@' or '/', and the host must be followed
		// by '/'. That trailing slash is what separates a DSN from an image
		// digest (app:v1@sha256:abc) or a pod reference (ns:pod@node), which
		// have no path after the host.
		pattern:     regexp.MustCompile(`\b[\w.-]+:[^\s@/]+@([\w.-]+/)`),
		replacement: redacted + "@${1}",
	},
	{
		// Authorization header values: "Bearer abc123", "Basic dXNlcjpwYXNz".
		//
		// Everything after the scheme word is redacted unconditionally. An
		// earlier version tried to tell a credential from an English word by
		// shape, so that "basic authentication is not enabled" kept its text.
		// Every such test has the same failure mode: a short, all-lowercase
		// opaque token, or unpadded base64 of a lowercase "user:pass", looks
		// exactly like a word and ships in clear. A lost word is a reporting
		// problem; a leaked token is a security one, so this errs toward
		// redacting.
		//
		// Known over-redaction: prose such as "bearer token has expired" and
		// "Basic Auth required" loses the word after the keyword. The scheme
		// word itself is kept, which usually leaves enough to recognise the
		// failure.
		pattern:     regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9\-._~+/=]+`),
		replacement: "${1} " + redacted,
	},
	{
		// key=value and key: value pairs for sensitive key names.
		//
		// \b is the left boundary rather than an explicit delimiter class.
		// Enumerating delimiters looks tidier but silently drops whatever the
		// list forgets, and the omissions bite hardest: ';' carries
		// ODBC/JDBC/libpq connection strings and '(' the parenthesised
		// contexts Go errors favour, both of which are prime credential
		// shapes. \b admits them all.
		//
		// The [\w.-]* prefix catches compound names that appear in OAuth, SSO
		// and AWS bodies - access_token, client_secret, aws_secret_access_key,
		// x-api-key, X-Amz-Signature. A bare \b before the alternation matches
		// none of them, because '_' is a word character so there is no
		// boundary before "token" in "access_token". The prefix sits inside
		// group 1 so the whole key name survives redaction.
		//
		// [_ \t-]? covers the spaced forms vendors use in prose ("API key"),
		// and the optional connector word catches "Incorrect API key
		// provided: sk-..." - the shape OpenAI-compatible servers return in a
		// 401 body. The separator uses \s* rather than horizontal space only,
		// so a pretty-printed JSON body with the value on the next line is
		// still covered.
		//
		// Known over-redaction: a message of the form "<key name>: <prose>"
		// loses its first word, as in "cannot fetch token: 401 Unauthorized",
		// and a URL path segment ending in a key name reads as a key. Both
		// follow from preferring redaction when the shape is ambiguous.
		//
		// The key name and separator are kept so a reader can tell which
		// credential the message was carrying; only the value is replaced.
		pattern: regexp.MustCompile(
			`(?i)\b([\w.-]*(?:password|passwd|token|secret|signature|api[_ \t-]?key` +
				`|access[_ \t-]?key|private[_ \t-]?key|credential))` +
				`((?:[ \t]+(?:provided|supplied|given|specified|used|value))?"?\s*[=:]\s*)` +
				`("[^"]*"|'[^']*'|[^\s,;&)}]+)`),
		replacement: "${1}${2}" + redacted,
	},
}

// replacementChar is U+FFFD REPLACEMENT CHARACTER, the Unicode standard's
// "undecodable input was here" marker. It substitutes each run of bytes that
// is not valid UTF-8.
//
// This matters because OTLP span status and event fields are proto3 strings,
// which the protobuf encoder refuses to marshal when they hold invalid UTF-8;
// the batch processor then drops the whole batch rather than the one span
// that caused it.
//
// Spelled as utf8.RuneError rather than a literal glyph so the source stays
// ASCII and the intent is legible without rendering the character.
const replacementChar = string(utf8.RuneError)

// sanitize strips credential material from a message and truncates it so error
// text recorded on spans cannot leak secrets to telemetry backends.
func sanitize(msg string) string {
	// Bound the work before doing any of it. The rules below run over the
	// whole string, so an unbounded input lets whoever controls it spend
	// server CPU proportional to its length - reachable from MCP tool
	// arguments and from downstream response bodies. Cutting here rather
	// than at the call sites keeps the guarantee in one place.
	//
	// The cut is far above maxMessageLen, so it cannot orphan the right-hand
	// terminator of a credential that would otherwise have been redacted
	// inside the part that survives truncation.
	if len(msg) > maxSanitizeInput {
		limit := maxSanitizeInput
		for limit > 0 && !utf8.RuneStart(msg[limit]) {
			limit--
		}
		msg = msg[:limit]
	}

	// Normalize before anything else. Truncating on a rune boundary is not
	// enough on its own: a downstream error can carry invalid bytes anywhere
	// in the message, including in messages short enough never to be
	// truncated. Snowflake and HTTP clients echo raw response bytes, which are
	// not guaranteed to be UTF-8.
	if !utf8.ValidString(msg) {
		msg = strings.ToValidUTF8(msg, replacementChar)
	}

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

// SafeAttributeValue prepares an untrusted string for use as a span
// attribute: credential material is redacted, invalid UTF-8 is normalized,
// and the result is capped at maxMessageLen.
//
// Span attributes have no length limit in the SDK by default, so anything
// derived from client input has to be bounded before it is recorded.
//
// Note that redaction must happen before truncation, not after: two of the
// three rules need a right-hand terminator, so cutting first can remove the
// '@' or the closing quote that made a secret matchable and leave a prefix of
// it in the result. sanitize bounds its own input, so doing it in this order
// costs nothing.

func SafeAttributeValue(value string) string {
	return sanitize(value)
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
