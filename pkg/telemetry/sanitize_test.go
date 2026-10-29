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
