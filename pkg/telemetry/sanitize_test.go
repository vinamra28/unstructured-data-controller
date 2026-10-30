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
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestSanitizeRedactsSecrets(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		mustRedact []string
		mustKeep   []string
	}{
		{
			name:       "url userinfo credentials",
			input:      "dial failed: postgres://admin:hunter2@db.internal:5432/mydb",
			mustRedact: []string{"admin", "hunter2"},
			mustKeep:   []string{"db.internal", "postgres://"},
		},
		{
			// A '@' inside the password must not terminate the match early,
			// or the tail of the password survives in the exported message.
			name:       "url password containing an at sign",
			input:      "dial failed: postgres://admin:pa@ss@db.internal:5432/mydb",
			mustRedact: []string{"admin", "pa@ss", "ss@db"},
			mustKeep:   []string{"db.internal", "postgres://"},
		},
		{
			// Whitespace bounds the userinfo match, so an unrelated '@' later
			// in the message must not be swallowed.
			name:       "email after a url is not over-redacted",
			input:      "postgres://u:p@host unreachable, page admin@example.com",
			mustRedact: []string{"u:p@host"},
			mustKeep:   []string{"admin@example.com", "unreachable"},
		},
		{
			name:       "bearer token in header dump",
			input:      "401 from API, sent Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.abc",
			mustRedact: []string{"eyJhbGciOiJIUzI1NiJ9.abc"},
			mustKeep:   []string{"401 from API"},
		},
		{
			name:       "basic auth credentials",
			input:      "auth rejected: Basic dXNlcjpwYXNzd29yZA==",
			mustRedact: []string{"dXNlcjpwYXNzd29yZA=="},
			mustKeep:   []string{"auth rejected"},
		},
		{
			name:       "password key value pair",
			input:      "connect error: user=svc password=s3cr3t host=snowflake",
			mustRedact: []string{"s3cr3t"},
			mustKeep:   []string{"host=snowflake", "user=svc"},
		},
		{
			name:       "api key with colon separator",
			input:      `embedding call failed: {"api_key": "sk-live-abc123", "model": "nomic"}`,
			mustRedact: []string{"sk-live-abc123"},
			mustKeep:   []string{"nomic"},
		},
		{
			name:       "token query parameter",
			input:      "GET /v1/embed?token=abcd1234&model=nomic returned 500",
			mustRedact: []string{"abcd1234"},
			mustKeep:   []string{"model=nomic", "returned 500"},
		},
		{
			// Underscore-prefixed key names are the common case in OAuth and
			// SSO error bodies, and this server is OAuth-authenticated. '_' is
			// a word character, so a plain \b before the key alternation
			// cannot match any of these.
			name:       "oauth access_token in a json body",
			input:      `token exchange failed: {"access_token":"ya29.SECRETVALUE","expires_in":3599}`,
			mustRedact: []string{"ya29.SECRETVALUE"},
			mustKeep:   []string{"token exchange failed", "expires_in"},
		},
		{
			name:       "oauth client_secret",
			input:      "sso rejected: client_id=udc client_secret=supersecret123",
			mustRedact: []string{"supersecret123"},
			mustKeep:   []string{"client_id=udc"},
		},
		{
			name:       "oauth refresh_token with colon separator",
			input:      "refresh failed: refresh_token: rt_abcdef123456",
			mustRedact: []string{"rt_abcdef123456"},
			mustKeep:   []string{"refresh failed"},
		},
		{
			name:       "aws secret access key",
			input:      "sts error: aws_secret_access_key=wJalrXUtnFEMI/K7MDENG host=s3",
			mustRedact: []string{"wJalrXUtnFEMI/K7MDENG"},
			mustKeep:   []string{"host=s3"},
		},
		{
			name:       "hyphenated header style key",
			input:      "upstream rejected: x-api-key=sk-live-9f8e7d",
			mustRedact: []string{"sk-live-9f8e7d"},
			mustKeep:   []string{"upstream rejected"},
		},
		{
			// The exact shape pkg/embedding records on a span: an
			// OpenAI-compatible 401 body echoing the presented key, with the
			// key name spelled with a space and a word before the separator.
			name:       "openai style 401 body echoing the key",
			input:      `API returned status 401: {"error":{"message":"Incorrect API key provided: sk-live-7f3a9c2e8b1d."}}`,
			mustRedact: []string{"sk-live-7f3a9c2e8b1d"},
			mustKeep:   []string{"401", "Incorrect API key"},
		},
		{
			name:       "spaced key name with title case",
			input:      "Invalid API Key: hf_QRsTuVwXyZ",
			mustRedact: []string{"hf_QRsTuVwXyZ"},
			mustKeep:   []string{"Invalid API Key"},
		},
		{
			// Snowflake DSNs carry userinfo with no scheme in front.
			name:       "scheme-less dsn userinfo",
			input:      "connection refused: user:Sup3rS3cret@acct/DB/PUBLIC",
			mustRedact: []string{"Sup3rS3cret"},
			mustKeep:   []string{"connection refused", "acct/DB/PUBLIC"},
		},
		{
			// Snowflake hands back pre-signed S3 URLs for chunk downloads.
			name:       "presigned url signature",
			input:      "fetch failed: https://x.s3.amazonaws.com/c?X-Amz-Credential=AKIA&X-Amz-Signature=deadbeefcafe123",
			mustRedact: []string{"deadbeefcafe123"},
			mustKeep:   []string{"fetch failed", "x.s3.amazonaws.com"},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			got := sanitize(testCase.input)

			for _, secret := range testCase.mustRedact {
				assert.NotContains(t, got, secret,
					"secret %q must not survive sanitization, got: %s", secret, got)
			}
			for _, keep := range testCase.mustKeep {
				assert.Contains(t, got, keep,
					"diagnostic context %q should be preserved, got: %s", keep, got)
			}
			assert.Contains(t, got, redacted, "a redaction marker should be present")
		})
	}
}

// TestSanitizeProducesExactOutput pins the full replacement text. The
// Contains/NotContains assertions above pass even when the non-secret part of
// a match is mangled, which is how a URL-only rewrite ("key[REDACTED]@") went
// unnoticed while it was being applied to bearer and key=value matches too.
func TestSanitizeProducesExactOutput(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "url keeps its scheme and host",
			input:    "dial failed: postgres://admin:hunter2@db.internal:5432/mydb",
			expected: "dial failed: postgres://[REDACTED]@db.internal:5432/mydb",
		},
		{
			name:     "bearer keeps the scheme word",
			input:    "401 from API, sent Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.abc",
			expected: "401 from API, sent Authorization: Bearer [REDACTED]",
		},
		{
			name:     "basic keeps the scheme word",
			input:    "auth rejected: Basic dXNlcjpwYXNzd29yZA==",
			expected: "auth rejected: Basic [REDACTED]",
		},
		{
			name:     "key=value keeps the key and separator",
			input:    "connect error: user=svc password=s3cr3t host=snowflake",
			expected: "connect error: user=svc password=[REDACTED] host=snowflake",
		},
		{
			// The JSON must stay structurally readable; an earlier version
			// emitted a fabricated '@' and ate the closing quote and brace.
			name:     "json body keeps its structure",
			input:    `embedding call failed: {"api_key": "sk-live-abc123", "model": "nomic"}`,
			expected: `embedding call failed: {"api_key": [REDACTED], "model": "nomic"}`,
		},
		{
			name:     "query parameter keeps the key and following params",
			input:    "GET /v1/embed?token=abcd1234&model=nomic returned 500",
			expected: "GET /v1/embed?token=[REDACTED]&model=nomic returned 500",
		},
		{
			name:     "single quoted value",
			input:    "dsn rejected: password='s3cr3t' sslmode=require",
			expected: "dsn rejected: password=[REDACTED] sslmode=require",
		},
		{
			name:     "multiple secrets in one message",
			input:    "token=aaa and password=bbb",
			expected: "token=[REDACTED] and password=[REDACTED]",
		},
		{
			// The full compound key is kept, not just the matched suffix, so
			// the reader can still tell which credential it was.
			name:     "compound key name survives redaction",
			input:    `{"access_token":"ya29.SECRET"}`,
			expected: `{"access_token":[REDACTED]}`,
		},
		{
			name:     "aws style key keeps its prefix",
			input:    "aws_secret_access_key=AKIAEXAMPLE",
			expected: "aws_secret_access_key=[REDACTED]",
		},
		{
			// A key that merely contains a sensitive word but does not end in
			// one is not a credential and must be left alone.
			name:     "non credential key is untouched",
			input:    "token_count=42",
			expected: "token_count=42",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.expected, sanitize(testCase.input))
		})
	}
}

func TestSanitizeLeavesBenignMessagesIntact(t *testing.T) {
	msg := "snowflake query failed: table CHUNKS does not exist in schema PUBLIC"
	assert.Equal(t, msg, sanitize(msg))
}

// An email address has no preceding colon, so the userinfo rule must leave it
// alone even when a real DSN appears earlier in the same message.
func TestSanitizeKeepsEmailAddresses(t *testing.T) {
	got := sanitize("postgres://u:p@host unreachable, page admin@example.com")

	assert.Contains(t, got, "admin@example.com")
	assert.NotContains(t, got, "u:p@")
}

func TestSanitizeTruncatesLongMessages(t *testing.T) {
	long := strings.Repeat("x", maxMessageLen*2)

	got := sanitize(long)

	require.LessOrEqual(t, len(got), maxMessageLen+len("...[truncated]"))
	assert.True(t, strings.HasSuffix(got, "...[truncated]"),
		"oversized messages should be marked as truncated")
}

func TestSanitizeEmptyMessage(t *testing.T) {
	assert.Empty(t, sanitize(""))
}

// TestSanitizeTruncatesOnARuneBoundary guards the OTLP export path: protobuf
// rejects invalid UTF-8 in proto3 string fields, and the batch processor drops
// the entire batch on an export failure, not just the offending span. This
// operator crawls Drive and Git, so non-ASCII error text is routine.
func TestSanitizeTruncatesOnARuneBoundary(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "japanese", input: strings.Repeat("日本語テキスト", 100)},
		{name: "emoji", input: strings.Repeat("🙂", 200)},
		{name: "accented latin", input: strings.Repeat("café-résumé-", 60)},
		{name: "mixed ascii and multibyte", input: strings.Repeat("path/到/文件/", 50)},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			got := sanitize(testCase.input)

			require.True(t, utf8.ValidString(got),
				"truncated message must stay valid UTF-8 or the whole span batch is dropped")
			assert.True(t, strings.HasSuffix(got, "...[truncated]"))
			// Back-off never drops more than the 3 continuation bytes of one rune.
			assert.Greater(t, len(got), maxMessageLen-4+len("...[truncated]"))
		})
	}
}

// TestSanitizeNormalizesInvalidUTF8 covers invalid bytes anywhere in the
// message, not just at the truncation boundary. Downstream clients echo raw
// response bytes that are not guaranteed to be UTF-8, and a short message
// never reaches the length cap at all, so boundary handling alone leaves the
// OTLP trace export rejecting the batch.
func TestSanitizeNormalizesInvalidUTF8(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "short message, never truncated", input: "snowflake error: \xff\xfe bad bytes"},
		{name: "invalid bytes before the cap", input: "err \xff\xfe " + strings.Repeat("a", 400)},
		{name: "lone continuation byte", input: "response: \x80"},
		{name: "truncated multibyte sequence mid-message", input: "prefix \xe6\x97 suffix"},
		{name: "invalid bytes around a secret", input: "\xffpassword=hunter2\xfe"},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			got := sanitize(testCase.input)
			assert.True(t, utf8.ValidString(got),
				"sanitize must emit valid UTF-8, got %q", got)
		})
	}
}

// Redaction has to keep working once invalid bytes have been replaced.
func TestSanitizeRedactsAroundInvalidUTF8(t *testing.T) {
	got := sanitize("\xffconnect failed: password=hunter2\xfe")

	require.True(t, utf8.ValidString(got))
	assert.NotContains(t, got, "hunter2")
	assert.Contains(t, got, redacted)
}

func TestSanitizeKeepsShortMultibyteMessagesIntact(t *testing.T) {
	msg := "ドキュメント処理に失敗しました"
	assert.Equal(t, msg, sanitize(msg))
}

func TestRecordSpanErrorSanitizesBeforeExport(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, tp.Shutdown(context.Background())) })

	_, span := tp.Tracer("test").Start(context.Background(), "downstream.call")
	RecordSpanError(span, errors.New("snowflake dial failed: postgres://svc:hunter2@db:5432/x"))
	span.End()

	ended := recorder.Ended()
	require.Len(t, ended, 1)

	require.Equal(t, codes.Error, ended[0].Status().Code)
	assert.NotContains(t, ended[0].Status().Description, "hunter2",
		"span status must not carry the raw credential")

	events := ended[0].Events()
	require.NotEmpty(t, events, "RecordError should add an exception event")
	for _, event := range events {
		for _, attr := range event.Attributes {
			assert.NotContains(t, attr.Value.AsString(), "hunter2",
				"span event attributes must not carry the raw credential")
		}
	}
}

func TestRecordSpanErrorIgnoresNil(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, tp.Shutdown(context.Background())) })

	_, span := tp.Tracer("test").Start(context.Background(), "downstream.call")
	RecordSpanError(span, nil)
	span.End()

	ended := recorder.Ended()
	require.Len(t, ended, 1)
	assert.NotEqual(t, codes.Error, ended[0].Status().Code,
		"a nil error must leave the span unmarked")
	assert.Empty(t, ended[0].Events(), "a nil error must not add an exception event")
}

// TestSanitizeCorpus is the regression corpus. Each shape below was verified
// to leak or to over-redact at some point during development, so the table is
// the record of what the three rules actually have to handle rather than a
// restatement of them.
func TestSanitizeCorpus(t *testing.T) {
	leaks := []struct {
		name   string
		input  string
		secret string
	}{
		{"basic auth with pure-alpha base64", `Basic dXNlcjpwYXNz`, "dXNlcjpwYXNz"},
		{"long lowercase bearer token", `Bearer abcdefghijklmnopqrst`, "abcdefghijklmnopqrst"},
		{"jwt bearer", `401, sent Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.abc`, "eyJhbGciOiJIUzI1NiJ9.abc"},
		{"padded base64 basic", `auth rejected: Basic dXNlcjpwYXNzd29yZA==`, "dXNlcjpwYXNzd29yZA=="},
		{"redis auth url, empty username", `redis://:hunter2@cache.internal:6379`, "hunter2"},
		{"git over https token", `https://:ghp_AAAABBBBCCCCDDDD@github.com/o/r.git`, "ghp_AAAABBBBCCCCDDDD"},
		{"postgres dsn", `dial failed: postgres://admin:hunter2@db.internal:5432/mydb`, "hunter2"},
		{"password containing an at sign", `postgres://admin:pa@ss@db.internal:5432/x`, "pa@ss"},
		{"scheme-less snowflake dsn", `connection refused: user:Sup3rS3cret@acct/DB/PUBLIC`, "Sup3rS3cret"},
		{
			"openai 401 echoing the key",
			`API returned status 401: {"error":{"message":"Incorrect API key provided: sk-live-7f3a9c2e8b1d."}}`,
			"sk-live-7f3a9c2e8b1d",
		},
		{"spaced title-case key name", `Invalid API Key: hf_QRsTuVwXyZ`, "hf_QRsTuVwXyZ"},
		{"connection string password", `connect error: user=svc password=s3cr3t host=snowflake`, "s3cr3t"},
		{"oauth json access_token", `{"access_token":"ya29.SECRETVALUE"}`, "ya29.SECRETVALUE"},
		{"oauth client_secret", `client_secret=supersecret123`, "supersecret123"},
		{"aws secret access key", `aws_secret_access_key=wJalrXUtnFEMI`, "wJalrXUtnFEMI"},
		{"token query parameter", `GET /v1/embed?token=abcd1234&model=nomic`, "abcd1234"},
		{"hyphenated header key", `x-api-key=sk-live-9f8e7d`, "sk-live-9f8e7d"},
		{
			"presigned url signature",
			`fetch failed: https://x.s3.amazonaws.com/c?X-Amz-Credential=AKIA&X-Amz-Signature=deadbeefcafe123`,
			"deadbeefcafe123",
		},
		{"single quoted password", `dsn rejected: password='s3cr3t' sslmode=require`, "s3cr3t"},
		{"password containing a comma", `dial failed: postgres://svc:Tr0ub4dor,3@db.internal:5432/app`, "Tr0ub4dor,3"},
		{"password containing braces", `postgres://svc:p{a}w@db/x`, "p{a}w"},
		{"odbc connection string", `odbc: Driver=snowflake;UID=svc;PWD=x;password=Tr0ub4dor;Host=h`, "Tr0ub4dor"},
		{"key in a parenthesised context", `failed to build client (api_key=sk-live-abc123): invalid`, "sk-live-abc123"},
		{"pretty printed json body", "{\n  \"password\":\n   \"hunter2\"\n}", "hunter2"},
		{"short all-letter bearer token", `401: Bearer tokenvalue`, "tokenvalue"},
		{"short all-letter basic token", `401: Basic abcdefghijkl`, "abcdefghijkl"},
	}

	for _, testCase := range leaks {
		t.Run("redacts/"+testCase.name, func(t *testing.T) {
			got := sanitize(testCase.input)
			assert.NotContains(t, got, testCase.secret,
				"credential survived sanitization: %s", got)
			assert.Contains(t, got, redacted)
		})
	}

	// Over-redaction is the other failure mode: a span whose whole purpose is
	// to carry the cause is worthless if the cause is replaced by a marker.
	intact := []struct{ name, input string }{
		{"jwt signature error", `token signature is invalid: signature is invalid`},
		{"signature verification error", `signature verification failed: invalid JWT`},
		{"wrapped multi-line error", "failed to refresh token\ncaused by: connection refused"},
		{"pem parse error", `private key is malformed: expected PEM block`},
		{"multi-line secret error", "Client secret\nis required: missing"},
		{"token mentioned with a subject", `Invalid token for user: alice`},
		{"key mentioned with a timestamp", `API key expired at: 2026-10-29`},
		{"compact json with an email", `{"error":"access_denied","account":"svc@corp.com"}`},
		{"key mentioned in prose", `the API key was rotated at 10:30 yesterday`},
		{"key name that is not a credential", `token_count=42`},
		{"snowflake table error", `snowflake query failed: table CHUNKS does not exist in schema PUBLIC`},
		{"docling timeout", `docling task failed: timeout after 30s`},
		{"missing file", `file not found: /stages/chunk/report.pdf`},
		{"context deadline", `context deadline exceeded while calling embedding service`},
		{"query param that is not a secret", `model=nomic returned 500`},
		{"elapsed time with a colon", `job completed in 1:30 elapsed`},
		{"plain https url", `HTTP 503 from https://api.example.com/v1/embed`},
		{"grpc status", `rpc error: code = Unavailable desc = connection error`},
		{"aws list error", `failed to list objects: operation error S3: ListObjectsV2, https response error StatusCode: 403`},
		{"image reference with a digest", `failed to pull image quay.io/redhat/udc:v1.2.0@sha256:deadbeef`},
		{"pod reference with a node", `Pod default:web-0@node-1 evicted`},
		{"git ref with a remote", `ref refs/heads/main:deploy@origin`},
		{"email in a query string", `GET http://host:8080?email=foo@bar.com failed`},
		{"at sign in a fragment", `https://api.example.com/v1#frag@notuser`},
	}

	for _, testCase := range intact {
		t.Run("keeps/"+testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.input, sanitize(testCase.input),
				"a redaction marker here would replace the diagnostic")
		})
	}
}

// TestSafeAttributeValueBoundsClientInput covers the span-attribute path,
// which is reachable directly from MCP tool arguments.
func TestSafeAttributeValueBoundsClientInput(t *testing.T) {
	t.Run("caps long values", func(t *testing.T) {
		got := SafeAttributeValue(strings.Repeat("a", 10_000))
		assert.Equal(t, strings.Repeat("a", maxMessageLen)+"...[truncated]", got)
	})

	t.Run("keeps short values whole", func(t *testing.T) {
		assert.Equal(t, "pipeline-a", SafeAttributeValue("pipeline-a"))
	})

	t.Run("redacts", func(t *testing.T) {
		got := SafeAttributeValue("password=hunter2")
		assert.NotContains(t, got, "hunter2")
		assert.Contains(t, got, redacted)
	})

	t.Run("cuts on a rune boundary", func(t *testing.T) {
		assert.True(t, utf8.ValidString(SafeAttributeValue(strings.Repeat("日本語", 500))),
			"must stay valid UTF-8 for the exporter")
	})

	// Redaction has to run before truncation. Two of the three rules need a
	// right-hand terminator, so cutting first removes the '@' that made the
	// credential matchable and leaves a prefix of the password behind.
	t.Run("a secret longer than the cap is not left half-redacted", func(t *testing.T) {
		secret := "SuperSecretPassword" + strings.Repeat("x", 400)
		got := SafeAttributeValue("postgres://admin:" + secret + "@db.internal:5432/mydb")
		assert.NotContains(t, got, "SuperSecretPassword")
		assert.Contains(t, got, redacted)
	})

	// Whoever controls the value must not be able to buy unbounded regex work.
	t.Run("input is bounded before scanning", func(t *testing.T) {
		assert.LessOrEqual(t, len(SafeAttributeValue(strings.Repeat("a", 4<<20))),
			maxMessageLen+len("...[truncated]"))
	})
}

// TestSanitizeKnownOverRedaction pins the cost of the rules erring toward
// redaction. Each case here loses a word that a reader would have wanted.
//
// They are asserted rather than fixed because every attempt to tell a
// credential from an English word by shape has the opposite failure: a short
// all-lowercase opaque token, or unpadded base64 of a lowercase "user:pass",
// is indistinguishable from prose and would ship in clear. A lost word is a
// reporting problem and a leaked token is a security one, so the rules take
// the reporting problem.
//
// If one of these starts mattering operationally, the fix is a narrower
// anchor (an Authorization-header context for bearer/basic, say), not a
// loosening of the match.
func TestSanitizeKnownOverRedaction(t *testing.T) {
	tests := []struct{ name, input, want string }{
		{
			name:  "basic as an english word",
			input: `basic authentication is not enabled for this account`,
			want:  `basic [REDACTED] is not enabled for this account`,
		},
		{
			name:  "bearer as an english word",
			input: `bearer token has expired, please re-authenticate`,
			want:  `bearer [REDACTED] has expired, please re-authenticate`,
		},
		{
			name:  "url path segment ending in a key name",
			input: `Get "https://oauth2.googleapis.com/token": dial tcp: i/o timeout`,
			want:  `Get "https://oauth2.googleapis.com/token": [REDACTED] tcp: i/o timeout`,
		},
		{
			name:  "key name followed by prose",
			input: `oauth2: cannot fetch token: 401 Unauthorized`,
			want:  `oauth2: cannot fetch token: [REDACTED] Unauthorized`,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.want, sanitize(testCase.input))
		})
	}
}
