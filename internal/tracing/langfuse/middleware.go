package langfuse

import (
	"context"
	"strconv"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
)

// GinMiddleware returns a Gin handler that opens a Langfuse trace for each
// incoming request that hits a traced path (see shouldTrace), or — when
// X-Langfuse-Trace-Id is present — resumes the upstream trace from xgimi AI Hub
// so WeKnora generations nest under the same Langfuse tree.
func GinMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		mgr := GetManager()
		if !mgr.Enabled() {
			c.Next()
			return
		}

		upstreamTrace, upstreamParent := incomingHubLangfuseTrace(c)
		forceTrace := upstreamTrace != ""

		if !shouldTrace(c) && !forceTrace {
			c.Next()
			return
		}

		ctx := c.Request.Context()
		userID := extractUserID(ctx)
		sessionID := extractSessionID(c)

		opts := TraceOptions{
			Name:      c.Request.Method + " " + c.FullPath(),
			UserID:    userID,
			SessionID: sessionID,
			Metadata: map[string]interface{}{
				"http.method": c.Request.Method,
				"http.path":   c.FullPath(),
				"http.query":  c.Request.URL.RawQuery,
			},
			Tags: []string{"http", strings.ToLower(c.Request.Method)},
		}
		if upstreamTrace != "" {
			opts.Metadata["hub.langfuse.linked"] = true
		}
		if rid, ok := types.RequestIDFromContext(ctx); ok {
			opts.Metadata["request_id"] = rid
		}

		var newCtx context.Context
		var trace *Trace

		if upstreamTrace != "" {
			var resumed *Trace
			newCtx, resumed = mgr.ResumeTrace(ctx, upstreamTrace, upstreamParent)
			if resumed == nil {
				newCtx, trace = mgr.StartTrace(ctx, opts)
			} else {
				trace = resumed
			}
		} else {
			newCtx, trace = mgr.StartTrace(ctx, opts)
		}

		c.Request = c.Request.WithContext(newCtx)

		c.Next()

		if trace != nil {
			trace.Finish(map[string]interface{}{
				"status": c.Writer.Status(),
			}, map[string]interface{}{
				"http.status_code": c.Writer.Status(),
				"response.size":    c.Writer.Size(),
			})
		}
	}
}

// HTTP headers from xgimi AI Hub (or any upstream) for Langfuse trace stitching.
// See backend/app/services/langfuse_service.py (get_propagation_headers).
const (
	headerLangfuseTraceID               = "X-Langfuse-Trace-Id"
	headerLangfuseParentObservationID   = "X-Langfuse-Parent-Observation-Id"
)

func incomingHubLangfuseTrace(c *gin.Context) (traceID, parentObs string) {
	traceID = strings.TrimSpace(c.GetHeader(headerLangfuseTraceID))
	parentObs = strings.TrimSpace(c.GetHeader(headerLangfuseParentObservationID))
	return traceID, parentObs
}

// shouldTrace restricts tracing to endpoints where LLM work (or the asynq
// jobs that will run LLM work) originates. Everything else — auth, list,
// config fetches, static assets, health checks — is skipped to keep the
// Langfuse dashboard's signal-to-noise ratio high.
//
// The list below is grouped by purpose:
//   - online inference: knowledge-chat / agent-chat / knowledge-search /
//     generate_title are the existing chat/retrieval surface.
//   - ingestion: POST on knowledge-bases/:id/knowledge (file / url / manual)
//     and reparse / move / copy all kick off asynq jobs that later run
//     embedding/VLM/chat calls. Tracing the HTTP side means the Langfuse UI
//     shows a parent trace whose children are the worker spans.
//   - batch ops: FAQ import + knowledge batch delete also enqueue jobs.
//   - model/setup diagnostics: initialization endpoints exercise live
//     models; evaluation runs arbitrary chat pipelines.
//   - wiki: auto-fix kicks off wiki ingest, which calls embedding.
//
// Read-only listing / GET endpoints are deliberately excluded; they never
// trigger LLM work and would only add noise.
func shouldTrace(c *gin.Context) bool {
	path := c.FullPath()
	if path == "" {
		return false
	}
	method := c.Request.Method
	// Online inference
	switch {
	case strings.HasPrefix(path, "/api/v1/knowledge-chat"),
		strings.HasPrefix(path, "/api/v1/agent-chat"),
		strings.HasPrefix(path, "/api/v1/knowledge-search"),
		strings.HasPrefix(path, "/api/v1/sessions") && strings.Contains(path, "generate_title"),
		strings.HasPrefix(path, "/api/v1/initialization/remote/check"),
		strings.HasPrefix(path, "/api/v1/initialization/embedding/test"),
		strings.HasPrefix(path, "/api/v1/initialization/rerank/check"),
		strings.HasPrefix(path, "/api/v1/initialization/asr/check"),
		strings.HasPrefix(path, "/api/v1/initialization/multimodal/test"),
		strings.HasPrefix(path, "/api/v1/initialization/extract/"),
		strings.HasPrefix(path, "/api/v1/evaluation"):
		return true
	}
	// Ingestion (all POST/PUT that enqueue LLM-backed async work)
	if method == "POST" || method == "PUT" {
		switch {
		// Per-knowledge-base ingestion surface.
		case strings.Contains(path, "/knowledge-bases/") && strings.Contains(path, "/knowledge/"):
			return true
		// Knowledge-level mutations that trigger re-processing.
		case strings.HasPrefix(path, "/api/v1/knowledge/") &&
			(strings.HasSuffix(path, "/reparse") ||
				strings.HasSuffix(path, "/move") ||
				strings.Contains(path, "/manual/")):
			return true
		// Knowledge base copy (clones an entire KB, fanning out documents).
		case path == "/api/v1/knowledge-bases/copy":
			return true
		// FAQ bulk import.
		case strings.Contains(path, "/faq/entries") ||
			strings.Contains(path, "/faq/entry") ||
			strings.Contains(path, "/faq/import"):
			return true
		// Wiki auto-fix enqueues wiki ingest.
		case strings.Contains(path, "/wiki/auto-fix") ||
			strings.Contains(path, "/wiki/rebuild-links"):
			return true
		// Chunk-level mutations that rerun embeddings on update.
		case strings.HasPrefix(path, "/api/v1/chunks/") && method == "PUT":
			return true
		// Manual data source sync triggers asynq sync.
		case strings.Contains(path, "/datasource/") && strings.HasSuffix(path, "/sync"):
			return true
		}
	}
	return false
}

func extractUserID(ctx context.Context) string {
	if v, ok := ctx.Value(types.UserIDContextKey).(string); ok && v != "" {
		return v
	}
	if v, ok := ctx.Value(types.TenantIDContextKey).(uint64); ok && v != 0 {
		return "tenant:" + strconv.FormatUint(v, 10)
	}
	return ""
}

func extractSessionID(c *gin.Context) string {
	if v := c.Param("session_id"); v != "" {
		return v
	}
	if v := c.Param("id"); v != "" && strings.Contains(c.FullPath(), "/sessions/") {
		return v
	}
	return ""
}
