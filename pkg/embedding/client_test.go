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

package embedding

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A non-200 body is vendor-controlled and travels further than a span: it is
// appended to the VectorEmbeddingsGenerator status condition and the
// controller log, so it must be both bounded and scrubbed.
func TestErrorBodyIsBoundedAndScrubbed(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		mustRedact string
	}{
		{
			name:       "openai style 401 echoing the key",
			body:       `{"error":{"message":"Incorrect API key provided: sk-live-7f3a9c2e8b1d."}}`,
			mustRedact: "sk-live-7f3a9c2e8b1d",
		},
		{
			name:       "upstream dsn in the body",
			body:       `backend unreachable: postgres://svc:hunter2@db:5432/x`,
			mustRedact: "hunter2",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = io.WriteString(w, testCase.body)
			}))
			defer server.Close()

			client := NewHTTPClient(&HTTPClientConfig{Endpoint: server.URL, ModelName: "m"})
			_, err := client.GenerateEmbeddings(context.Background(), []string{"hello"}, "")

			require.Error(t, err)
			assert.Contains(t, err.Error(), "401")
			assert.NotContains(t, err.Error(), testCase.mustRedact,
				"the credential reaches a CR status condition, not just a span")
		})
	}
}

func TestErrorBodyIsTruncated(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, strings.Repeat("x", 1<<20))
	}))
	defer server.Close()

	client := NewHTTPClient(&HTTPClientConfig{Endpoint: server.URL, ModelName: "m"})
	_, err := client.GenerateEmbeddings(context.Background(), []string{"hello"}, "")

	require.Error(t, err)
	assert.Less(t, len(err.Error()), 1024,
		"a megabyte of vendor HTML must not end up in an error, a log line and a CR status")
}

// An over-long success body must be reported as itself, not as a JSON syntax
// error that says nothing about the cause and recurs on every retry.
func TestOversizedResponseIsReportedClearly(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"data":[`)
		chunk := strings.Repeat(`{"embedding":[0.1,0.2,0.3]},`, 4096)
		for written := 0; written < maxResponseBytes+len(chunk); written += len(chunk) {
			if _, err := io.WriteString(w, chunk); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	client := NewHTTPClient(&HTTPClientConfig{Endpoint: server.URL, ModelName: "m"})
	_, err := client.GenerateEmbeddings(context.Background(), []string{"hello"}, "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds")
}
