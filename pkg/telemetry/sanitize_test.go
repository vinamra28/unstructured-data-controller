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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
