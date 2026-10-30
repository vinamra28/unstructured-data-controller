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

package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	mcptools "github.com/redhat-data-and-ai/unstructured-data-controller/internal/mcp/tools"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/auth"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/embedding"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/k8sclient"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/logger"
	udctelemetry "github.com/redhat-data-and-ai/unstructured-data-controller/pkg/telemetry"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	serverName     = "unstructured-data-controller"
	defaultAddr    = ":8080"
	defaultMetrics = ":8000"

	// Shutdown is budgeted against the pod's default terminationGracePeriodSeconds
	// of 30s (config/mcp/deployment.yaml sets none). The two HTTP servers stop
	// concurrently within httpShutdownTimeout, then telemetry flushes within
	// flushTimeout, so the worst case is well under the grace period. Running
	// them serially on a budget each would push the flush - the stage this
	// ordering exists to protect - past the point the kubelet SIGKILLs.
	httpShutdownTimeout = 10 * time.Second
	flushTimeout        = 5 * time.Second
)

// version is the build version reported as the service.version resource
// attribute on every metric and span. Overridden at build time with
// -ldflags "-X main.version=..."; see the Makefile.
var version = "dev"

// shutdownHTTPServers stops every server concurrently on one shared deadline
// and logs any failure. Concurrent because the servers are independent: the
// MCP listener has no WriteTimeout by design (SSE needs long-lived
// responses), so letting it run first and alone would spend the whole budget
// before the others started.
func shutdownHTTPServers(timeout time.Duration, servers map[string]func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var wg sync.WaitGroup
	for name, shutdown := range servers {
		wg.Go(func() {
			if err := shutdown(ctx); err != nil {
				slog.Error("shutdown error", "server", name, "error", err)
			}
		})
	}
	wg.Wait()
}

func main() {
	logger.Init()
	slogLogger := logr.FromSlogHandler(slog.Default().Handler())
	ctrl.SetLogger(slogLogger)

	// Initialize OTel metrics and tracing. Prometheus metrics are served on /metrics.
	// OTLP push (for Datadog/Langfuse) activates when OTEL_EXPORTER_OTLP_ENDPOINT is set.
	metricsProvider, err := udctelemetry.Init(
		context.Background(), udctelemetry.Config{
			ServiceName:    serverName,
			ServiceVersion: version,
			// Tool calls are user-driven and low volume compared to reconcile
			// loops, and Langfuse usage analytics need every invocation to get
			// accurate counts. Export all traces.
			TraceSampleRatio: 1.0,
			// Send SDK diagnostics through the same slog pipeline as the rest
			// of the server, so a failing exporter appears in the JSON logs
			// rather than only on stderr.
			Logger: slogLogger.WithName("otel"),
		},
	)
	if err != nil {
		slog.Error("failed to initialize metrics provider", "error", err)
		os.Exit(1)
	}

	oauthCfg, err := auth.NewOAuthConfigFromEnv()
	if err != nil {
		slog.Error("failed to load OAuth configuration", "error", err)
		os.Exit(1)
	}

	provider, err := auth.NewGenericProvider(oauthCfg)
	if err != nil {
		slog.Error("failed to create OAuth provider", "error", err)
		os.Exit(1)
	}

	k8sClient, err := k8sclient.NewClient()
	if err != nil {
		slog.Error("failed to create kubernetes client", "error", err)
		os.Exit(1)
	}
	slog.Info("kubernetes client initialized successfully")

	mcpServer := mcp.NewServer(
		&mcp.Implementation{
			Name:    serverName,
			Version: version,
		},
		nil,
	)

	// Record call metrics and a trace span for every tool, including any added
	// later, rather than threading an observer through each handler.
	mcpServer.AddReceivingMiddleware(mcptools.TelemetryMiddleware(metricsProvider))

	embeddingClient := embedding.NewHTTPClient(&embedding.HTTPClientConfig{
		Endpoint:   os.Getenv("EMBEDDING_ENDPOINT"),
		APIKey:     os.Getenv("EMBEDDING_API_KEY"),
		AuthFormat: "Bearer",
		ModelName:  os.Getenv("EMBEDDING_MODEL_NAME"),
	})

	mcptools.RegisterListPipelines(mcpServer, k8sClient, metricsProvider)
	mcptools.RegisterGetChunksForEmbeddings(mcpServer, k8sClient, embeddingClient, metricsProvider)
	mcptools.RegisterGetProcessedDocument(mcpServer, k8sClient, metricsProvider)

	oauthStore := auth.NewOAuthStore()
	oauthMiddleware := auth.NewMiddleware(provider, slog.Default(), oauthCfg.DisableIntrospection)
	oauthServer := auth.NewOAuthServer(provider, oauthCfg.CallbackURL, oauthStore, slog.Default())

	mcpHandler := mcp.NewStreamableHTTPHandler(
		func(_ *http.Request) *mcp.Server { return mcpServer },
		nil,
	)

	mux := http.NewServeMux()
	protectedMCP := oauthMiddleware.Authenticate(mcpHandler)
	filteredMCP := mcptools.PipelineFilterMiddleware(protectedMCP)
	mux.Handle("/mcp", filteredMCP)
	mux.Handle("/mcp/{$}", filteredMCP) // match with trailing slash too

	// OAuth discovery (unauthenticated)
	mux.Handle("/.well-known/oauth-protected-resource", oauthMiddleware.ProtectedResourceMetadataHandler())
	mux.Handle("/.well-known/oauth-authorization-server", oauthMiddleware.MetadataHandler())

	// OAuth authorization server endpoints (unauthenticated)
	mux.HandleFunc("/auth/register", oauthServer.HandleRegister)
	mux.HandleFunc("/auth/authorize", oauthServer.HandleAuthorize)
	mux.HandleFunc("/auth/callback/oidc", oauthServer.HandleCallback)
	mux.HandleFunc("/auth/complete/{token}", oauthServer.HandleComplete)
	mux.HandleFunc("/auth/token", oauthServer.HandleToken)

	mux.HandleFunc("/healthz", healthHandler)
	mux.HandleFunc("/readyz", healthHandler)

	addr := defaultAddr
	if port := os.Getenv("MCP_SERVER_PORT"); port != "" {
		addr = ":" + port
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		// WriteTimeout intentionally unset: Streamable HTTP uses SSE which requires long-lived responses.
	}

	// Metrics are served on a dedicated port so scraping tools cannot reach
	// the MCP protocol, OAuth, or health endpoints through the metrics port.
	metricsAddr := defaultMetrics
	if port := os.Getenv("METRICS_PORT"); port != "" {
		metricsAddr = ":" + port
	}
	metricsMux := http.NewServeMux()
	if h := metricsProvider.PrometheusHandler(); h != nil {
		metricsMux.Handle("/metrics", h)
	}
	metricsSrv := &http.Server{
		Addr:              metricsAddr,
		Handler:           metricsMux,
		ReadHeaderTimeout: 10 * time.Second,
		// /metrics is unauthenticated and returns a finite, non-streaming
		// response. IdleTimeout does not bound an active write, so without this
		// a slow scrape client could hold the connection open indefinitely.
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		slog.Info("MCP server starting", "addr", addr, "endpoint", "/mcp")
		// errors.Is rather than ==: any future wrapping of the sentinel would
		// otherwise turn a normal shutdown into a non-zero exit.
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server failed to start", "error", err)
			os.Exit(1)
		}
	}()

	go func() {
		slog.Info("metrics server starting", "addr", metricsAddr)
		// An observability endpoint that cannot bind must not take down a
		// healthy server: /mcp, OAuth and /healthz are unaffected by it. An
		// os.Exit here would also bypass the graceful shutdown below, cutting
		// live SSE sessions mid-stream over a port conflict. Telemetry still
		// reaches an OTLP collector when one is configured.
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server stopped; Prometheus scraping is unavailable", "error", err)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down MCP server")

	// Close active MCP sessions concurrently - terminates SSE streams so
	// Shutdown doesn't block. Shutdown is called first to stop accepting
	// new connections, preventing new sessions from sneaking in.
	go func() {
		for session := range mcpServer.Sessions() {
			_ = session.Close()
		}
	}()

	// Each stage gets its own budget rather than sharing one. srv has no
	// WriteTimeout by design (SSE needs long-lived responses), and a tool call
	// can sit in the Snowflake retry loop for 9s of backoff alone, so a single
	// shared deadline is routinely exhausted by srv.Shutdown. Everything after
	// it would then run on an already-expired context: the OTLP flush would
	// return ctx.Err() immediately and drop the final metric interval and
	// every buffered span - precisely the telemetry for the shutdown you were
	// trying to understand.
	shutdownHTTPServers(httpShutdownTimeout, map[string]func(context.Context) error{
		"mcp":     srv.Shutdown,
		"metrics": metricsSrv.Shutdown,
	})

	// Flush pending OTLP metrics/traces before exit, on a fresh deadline.
	flushCtx, cancelFlush := context.WithTimeout(context.Background(), flushTimeout)
	if err := metricsProvider.Shutdown(flushCtx); err != nil {
		slog.Error("metrics provider shutdown error", "error", err)
	}
	cancelFlush()
	oauthMiddleware.Close()
	oauthStore.Close()
	slog.Info("MCP server stopped")
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
