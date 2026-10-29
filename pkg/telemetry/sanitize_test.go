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
