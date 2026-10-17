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
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/auth"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/embedding"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/k8sclient"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/logger"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/reranker"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"
)

type getChunksArgs struct {
	PipelineName   string `json:"pipeline_name" jsonschema:"Name of the UnstructuredDataPipeline. If not known, call list_unstructured_data_pipelines_for_user first and pick the matching pipeline based on its description."`
	Query          string `json:"query" jsonschema:"The search query to find relevant chunks using vector cosine similarity."`
	FilePathFilter string `json:"file_path_filter,omitempty" jsonschema:"Optional glob pattern to scope the search to files matching this path. Use * to match any path segment. Examples: '*/README.md', 'repo-name/docs/*', '*/aemv2/*'."`
	Limit          int    `json:"limit,omitempty" jsonschema:"Number of chunks to return. Use a lower value (1-5) for specific factual questions. Use a higher value for broad questions like summaries or 'tell me everything about X'. Defaults to 10 if not specified."`
}

const (
	// rerankerOverFetchMultiplier controls how many more candidates to retrieve
	// from Snowflake when reranking is enabled, giving the cross-encoder a
	// broader set to re-score before truncating to the requested limit.
	rerankerOverFetchMultiplier = 3

	// minRerankerCandidates floors the candidate pool so that small limits still
	// get a meaningful pool to re-score. Without it a limit of 1 would rerank
	// only 3 chunks, which is precisely the case where a wide pool helps most.
	minRerankerCandidates = 50

	// defaultChunkLimit is used when the caller does not specify a limit.
	defaultChunkLimit = 10

	// maxChunkLimit caps the user-supplied limit to prevent unbounded Snowflake
	// queries and oversized reranker payloads (especially after over-fetching).
	maxChunkLimit = 100

	// maxRerankerCandidates caps the number of documents sent to the reranker.
	// Most cross-encoder endpoints (vLLM, Cohere, TEI) cap at 100-256 documents.
	maxRerankerCandidates = 200

	// maxRerankerDocumentRunes caps the text sent per candidate so one oversized
	// chunk cannot blow the payload or overflow the model's window. Sized against
	// Qwen3-Reranker-4B's 8,192-token context, which covers query plus document
	// per pair. The budget is in runes rather than tokens because the ratio is
	// language-dependent: Latin text runs ~4 runes per token, CJK closer to 1, so
	// this holds even in the worst case while leaving room for the query.
	maxRerankerDocumentRunes = 6000
)

func RegisterGetChunksForEmbeddings(s *mcp.Server, k8sClient *k8sclient.Client, embeddingClient *embedding.HTTPClient, rerankerClient reranker.Reranker) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "get_chunks_for_embeddings",
		Description: `Search for relevant text chunks in a pipeline's data product using vector cosine similarity. Optionally scope the search to specific files using file_path_filter with glob patterns (e.g. "*/README.md", "*/aemv2/*").
If pipeline_name is not known, call list_unstructured_data_pipelines_for_user first and follow the instructions in its response.
If the returned chunks are not sufficient to answer the user's question, you may call get_processed_document with the same pipeline_name and the file_id from the top matching chunk to retrieve the full processed document for more context.
On error: report the exact error to the user and STOP. Do NOT retry with other pipelines.
On follow-up: if the user is not satisfied, ask them which pipeline to search. Do NOT automatically try other pipelines.`,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args getChunksArgs) (*mcp.CallToolResult, any, error) {
		username := ""
		if tokenInfo, ok := auth.TokenInfoFromContext(ctx); ok {
			username = tokenInfo.Username
		}
		ctx = logger.NewContext(ctx, uuid.NewString(), "get_chunks_for_embeddings", username)
		log := logger.FromContext(ctx)

		log.Info("tool invoked", "pipeline_name", args.PipelineName)

		if args.PipelineName == "" || args.Query == "" {
			log.Error("missing required parameters", "pipeline_name", args.PipelineName, "query_empty", args.Query == "")
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: "Error: pipeline_name and query are required. Call list_unstructured_data_pipelines_for_user first to get the pipeline name."}},
				IsError: true,
			}, nil, nil
		}

		if !isPipelineAllowed(ctx, args.PipelineName) {
			log.Error("pipeline not in per-client allow-list", "pipeline_name", args.PipelineName)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error: pipeline %q is not accessible in this session.", args.PipelineName)}},
				IsError: true,
			}, nil, nil
		}

		oauthToken, ok := auth.AccessTokenFromContext(ctx)
		if !ok {
			log.Error("oauth token not found in context")
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: errOAuthTokenNotFound}},
				IsError: true,
			}, nil, nil
		}

		if k8sClient == nil {
			log.Error("kubernetes client is nil")
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: "Error: kubernetes client is not initialized"}},
				IsError: true,
			}, nil, nil
		}

		qc, err := k8sClient.GetPipelineQueryConfig(ctx, args.PipelineName, operatorv1alpha1.StageTypeVectorEmbeddingsGenerator)
		if err != nil {
			log.Error("failed to get pipeline query config", "error", err)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error resolving pipeline %q: %v", args.PipelineName, err)}},
				IsError: true,
			}, nil, nil
		}

		if qc.Database == "" || qc.Schema == "" || qc.Table == "" {
			log.Error("pipeline query config has empty fields", "database", qc.Database, "schema", qc.Schema, "table", qc.Table)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error: pipeline %q has incomplete Snowflake query configuration", args.PipelineName)}},
				IsError: true,
			}, nil, nil
		}

		log.Info("generating embedding for query")
		result, err := embeddingClient.GenerateEmbeddings(ctx, []string{args.Query}, "float")
		if err != nil {
			log.Error("failed to generate embedding", "error", err)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error generating embedding: %v", err)}},
				IsError: true,
			}, nil, nil
		}

		if result == nil || result.Count == 0 || len(result.Embeddings) == 0 {
			log.Error("embedding API returned no vectors")
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: "Error: embedding API returned no vectors"}},
				IsError: true,
			}, nil, nil
		}
		log.Info("embedding generated", "vector_dimensions", len(result.Embeddings[0]))
		vectorLiteral := formatVectorLiteral(result.Embeddings[0])

		searchConfig := chunkSearchConfig{
			oauthToken:    oauthToken,
			database:      strings.ToUpper(strings.ReplaceAll(qc.Database, "-", "_")),
			schema:        strings.ToUpper(qc.Schema),
			table:         strings.ToUpper(qc.Table),
			vectorLiteral: vectorLiteral,
			query:         args.Query,
			filePattern:   convertFileGlobToSQL(args.FilePathFilter),
			limit:         resolveLimit(args.Limit),
		}
		searchConfig.fetchLimit = resolveFetchLimit(searchConfig.limit, rerankerClient != nil)

		chunks, err := retrieveChunks(ctx, searchConfig, rerankerClient)
		if err != nil {
			log.Error("failed to search chunks in snowflake", "error", err, "database", searchConfig.database, "schema", searchConfig.schema, "table", searchConfig.table)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error searching chunks: %v", err)}},
				IsError: true,
			}, nil, nil
		}

		jsonBytes, err := json.Marshal(chunks)
		if err != nil {
			log.Error("failed to marshal result", "error", err)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error marshaling result: %v", err)}},
				IsError: true,
			}, nil, nil
		}

		log.Info("completed successfully", "pipeline", args.PipelineName, "chunks_found", len(chunks))

		var nextStep string
		if len(chunks) > 0 {
			nextStep = fmt.Sprintf(
				"\n\nTIP: To retrieve the full processed document, call get_processed_document with pipeline_name=%q and the file_id from the most relevant chunk above.",
				args.PipelineName,
			)
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{
				Text: fmt.Sprintf("Found %d chunks for query in pipeline %q:\n%s%s", len(chunks), args.PipelineName, string(jsonBytes), nextStep),
			}},
		}, nil, nil
	})
}

// chunkSearchConfig carries the resolved Snowflake coordinates and the tuning
// knobs for a single chunk retrieval.
type chunkSearchConfig struct {
	oauthToken    string
	database      string
	schema        string
	table         string
	vectorLiteral string
	query         string
	filePattern   string
	limit         int
	fetchLimit    int
}

// resolveLimit applies the default and the hard cap to a caller-supplied limit.
func resolveLimit(requested int) int {
	if requested <= 0 {
		return defaultChunkLimit
	}
	return min(requested, maxChunkLimit)
}

// resolveFetchLimit widens the candidate pool when reranking is enabled so the
// cross-encoder has more to re-score than the caller ultimately wants back.
func resolveFetchLimit(limit int, rerankEnabled bool) int {
	if !rerankEnabled {
		return limit
	}
	return min(max(limit*rerankerOverFetchMultiplier, minRerankerCandidates), maxRerankerCandidates)
}

// searchVector runs the vector similarity leg, scoped by file pattern when set.
func searchVector(ctx context.Context, cfg chunkSearchConfig) ([]snowflake.ChunkResult, error) {
	log := logger.FromContext(ctx)
	if cfg.filePattern != "" {
		log.Info("searching snowflake with file filter", "database", cfg.database, "schema", cfg.schema, "table", cfg.table, "filePattern", cfg.filePattern, "limit", cfg.fetchLimit)
		return snowflake.SearchChunksWithFileFilter(ctx, cfg.oauthToken, cfg.database, cfg.schema, cfg.table, cfg.vectorLiteral, cfg.filePattern, cfg.fetchLimit)
	}
	log.Info("searching snowflake", "database", cfg.database, "schema", cfg.schema, "table", cfg.table, "limit", cfg.fetchLimit)
	return snowflake.SearchChunks(ctx, cfg.oauthToken, cfg.database, cfg.schema, cfg.table, cfg.vectorLiteral, cfg.fetchLimit)
}

// retrieveChunks runs vector retrieval and optionally reranks the candidates
// with a cross-encoder.
func retrieveChunks(ctx context.Context, cfg chunkSearchConfig, rerankerClient reranker.Reranker) ([]snowflake.ChunkResult, error) {
	log := logger.FromContext(ctx)

	chunks, err := searchVector(ctx, cfg)
	if err != nil {
		return nil, err
	}

	if rerankerClient != nil && len(chunks) > 0 {
		reranked, rerankErr := rerankChunks(ctx, rerankerClient, cfg.query, chunks, cfg.limit)
		if rerankErr != nil {
			// Reranking is best-effort: the cosine ordering is still valid, and
			// rerankChunks hands back the untouched candidates.
			log.Error("reranking failed, returning pre-rerank results", "error", rerankErr)
		}
		chunks = reranked
	}

	if len(chunks) > cfg.limit {
		chunks = chunks[:cfg.limit]
	}

	return chunks, nil
}

func formatVectorLiteral(vec []float64) string {
	parts := make([]string, len(vec))
	for i, v := range vec {
		parts[i] = strconv.FormatFloat(v, 'f', -1, 64)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

// convertFileGlobToSQL converts a file path glob pattern to a SQL LIKE pattern,
// escaping literal SQL wildcards (% and _) before converting glob * to %.
func convertFileGlobToSQL(glob string) string {
	if glob == "" {
		return ""
	}
	pattern := strings.ReplaceAll(glob, "%", "\\%")
	pattern = strings.ReplaceAll(pattern, "_", "\\_")
	return strings.ReplaceAll(pattern, "*", "%")
}

// truncateForReranker trims chunk text to the cross-encoder's usable window.
// Truncation is on a rune boundary so multi-byte characters are never split.
func truncateForReranker(text string) string {
	// Byte length is an upper bound on rune count, so this fast path avoids
	// allocating a rune slice for the common case of short chunks.
	if len(text) <= maxRerankerDocumentRunes {
		return text
	}
	runes := []rune(text)
	if len(runes) <= maxRerankerDocumentRunes {
		return text
	}
	return string(runes[:maxRerankerDocumentRunes])
}

// rerankChunks sends the chunk texts through a cross-encoder reranker,
// assigns reranker scores, re-sorts by relevance, and truncates to limit.
func rerankChunks(ctx context.Context, rerankerClient reranker.Reranker, query string, chunks []snowflake.ChunkResult, limit int) ([]snowflake.ChunkResult, error) {
	log := logger.FromContext(ctx)

	documents := make([]string, len(chunks))
	for i, chunk := range chunks {
		documents[i] = truncateForReranker(chunk.ChunkText)
	}

	results, err := rerankerClient.Rerank(ctx, query, documents, limit)
	if err != nil {
		return chunks, fmt.Errorf("calling reranker: %w", err)
	}

	log.Info("reranking applied", "candidates", len(chunks), "results", len(results))

	seenIndices := make(map[int]bool, len(results))
	reranked := make([]snowflake.ChunkResult, 0, len(results))
	for _, result := range results {
		if result.Index < 0 || result.Index >= len(chunks) {
			continue
		}
		if seenIndices[result.Index] {
			continue
		}
		seenIndices[result.Index] = true
		chunk := chunks[result.Index]
		score := result.RelevanceScore
		chunk.RerankerScore = &score
		reranked = append(reranked, chunk)
	}

	if len(reranked) == 0 {
		return chunks, errors.New("reranker returned no valid results")
	}

	sort.Slice(reranked, func(i, j int) bool {
		return *reranked[i].RerankerScore > *reranked[j].RerankerScore
	})

	if len(reranked) > limit {
		reranked = reranked[:limit]
	}

	return reranked, nil
}
