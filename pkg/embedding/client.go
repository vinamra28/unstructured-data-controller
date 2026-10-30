package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	udctelemetry "github.com/redhat-data-and-ai/unstructured-data-controller/pkg/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	HTTPClientTimeout = 60 * time.Second

	// maxResponseBytes caps how much of an embedding response this process
	// will read, so an unhealthy or hostile endpoint cannot drive it to
	// allocate without bound.
	//
	// It has to clear a legitimate worst case by a wide margin. At the
	// default batch size of 1000 (api/v1alpha1.DefaultEmbeddingBatchSize,
	// and the CRD sets no maximum), a JSON response runs about 20 MiB at 1024
	// dimensions, 60 MiB at 3072 and 81 MiB at 4096 - and 3072 and 4096 are
	// mainstream model sizes. A cap below that would reject ordinary traffic
	// at the shipped default.
	maxResponseBytes = 256 << 20

	// maxErrorBodyBytes caps how much of a non-200 body is read in order to
	// quote it. Vendor-controlled text that travels with an error reaches a
	// span, the controller log and a CR status condition, so both its volume
	// and its content are bounded - see the call site.
	maxErrorBodyBytes = 512
)

type EmbeddingGenerator interface {
	GenerateEmbeddings(ctx context.Context, inputs []string, encodingFormat string) (*EmbeddingResult, error)
}

// EmbeddingResult holds the embedding vectors and count returned by GenerateEmbeddings.
type EmbeddingResult struct {
	Embeddings [][]float64
	Count      int
}

// EmbeddingRequest is the JSON body for the embedding API (OpenAI-compatible).
type EmbeddingRequest struct {
	Model          string   `json:"model"`
	Input          []string `json:"input"`
	EncodingFormat string   `json:"encoding_format,omitempty"`
}

// EmbeddingData is a single embedding entry in the API response.
type EmbeddingData struct {
	Embedding []float64 `json:"embedding"`
}

// EmbeddingResponse is the JSON response from the embedding API.
type EmbeddingResponse struct {
	Data []EmbeddingData `json:"data"`
}

// HTTPClientConfig configures the HTTP embedding client.
type HTTPClientConfig struct {
	Endpoint   string
	AuthFormat string
	APIKey     string
	ModelName  string
}

type HTTPClient struct {
	Client *http.Client
	Config *HTTPClientConfig
}

func NewHTTPClient(config *HTTPClientConfig) *HTTPClient {
	return &HTTPClient{
		Client: &http.Client{
			Timeout: HTTPClientTimeout,
		},
		Config: config,
	}
}

// createHTTPRequest creates an HTTP request with auth header set from format and api key.
func (c *HTTPClient) createHTTPRequest(
	ctx context.Context, method, endpoint string, payload []byte,
) (*http.Request, error) {
	var body io.Reader
	if len(payload) > 0 {
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if c.Config.APIKey != "" && c.Config.AuthFormat != "" {
		req.Header.Set("Authorization", fmt.Sprintf("%s %s", c.Config.AuthFormat, c.Config.APIKey))
	}
	return req, nil
}

func (c *HTTPClient) GenerateEmbeddings(
	ctx context.Context, inputs []string, encodingFormat string,
) (result *EmbeddingResult, err error) {
	ctx, span := otel.Tracer("pkg/embedding").Start(ctx, "embedding.generate",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("embedding.model", c.Config.ModelName),
			attribute.Int("embedding.input_count", len(inputs)),
		))
	defer func() {
		udctelemetry.RecordSpanError(span, err)
		span.End()
	}()
	logger := log.FromContext(ctx)
	if len(inputs) == 0 {
		logger.Info("no inputs provided")
		return &EmbeddingResult{Embeddings: nil, Count: 0}, nil
	}

	payload, err := json.Marshal(EmbeddingRequest{
		Input:          inputs,
		EncodingFormat: encodingFormat,
		Model:          c.Config.ModelName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal embedding request: %w", err)
	}

	// TODO: Add a better log statement
	logger.Info("sending embedding request")
	req, err := c.createHTTPRequest(ctx, http.MethodPost, c.Config.Endpoint, payload)
	if err != nil {
		return nil, err
	}

	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send embedding request: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			logger.Error(err, "failed to close response body")
		}
	}()

	// The status check comes before any read of the body, so a 502 HTML page
	// or a hostile endpoint cannot make this process buffer megabytes just to
	// produce a short quote.
	if resp.StatusCode != http.StatusOK {
		// Read only what will be quoted. SafeAttributeValue both bounds and
		// scrubs: the body is vendor-controlled, OpenAI-compatible servers
		// echo the presented API key back in 401 bodies, and this error does
		// not only reach a span. It is also written verbatim into the
		// VectorEmbeddingsGenerator status condition
		// (api/v1alpha1.UpdateStatus appends err.Error() to the message), so
		// it is readable by anyone with get on the CR. The span sanitizer
		// does not cover that path; scrubbing here does.
		errBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
		if readErr != nil {
			return nil, fmt.Errorf("API returned status %d (error body unreadable: %w)",
				resp.StatusCode, readErr)
		}
		return nil, fmt.Errorf("API returned status %d: %s",
			resp.StatusCode, udctelemetry.SafeAttributeValue(string(errBody)))
	}

	// Decode as a stream rather than buffering the whole body first. At the
	// default batch size of 1000 a 4096-dimension response is around 80 MiB,
	// and io.ReadAll reaches that by repeated doubling - roughly 1.4 GiB of
	// cumulative allocation with 500 MiB live at the final copy, against the
	// MCP server's 512Mi limit. Streaming keeps the peak at the decoder's
	// buffer plus the result.
	//
	// LimitedReader is kept addressable so an over-long body is reported as
	// itself rather than as "unexpected end of JSON input", which says
	// nothing about the cause and would recur on every retry.
	limited := &io.LimitedReader{R: resp.Body, N: maxResponseBytes + 1}
	var embResp EmbeddingResponse
	if err := json.NewDecoder(limited).Decode(&embResp); err != nil {
		if limited.N == 0 {
			return nil, fmt.Errorf("embedding response exceeds %d bytes", maxResponseBytes)
		}
		return nil, fmt.Errorf("failed to decode embedding response: %w", err)
	}

	embeddings := make([][]float64, len(embResp.Data))
	for i := range embResp.Data {
		embeddings[i] = embResp.Data[i].Embedding
	}

	return &EmbeddingResult{
		Embeddings: embeddings,
		Count:      len(embeddings),
	}, nil
}
