// Package handler provides HTTP handler implementations for the WeKnora API.
// external.go exposes /api/v1/external/* routes consumed exclusively by xgimi AI Hub
// (and other internal automation). Authentication follows the global auth middleware
// (Bearer first, then X-API-Key). When a request arrives with X-API-Key +
// X-External-User-Email, this handler resolves the email to a real User and
// overwrites the synthetic system user in gin/request context so all downstream
// KB permission checks behave identically to a normal user Bearer request.
package handler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"

	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
)

// ExternalHandler handles /api/v1/external/* requests from xgimi AI Hub.
type ExternalHandler struct {
	userService      interfaces.UserService
	tenantService    interfaces.TenantService
	kbService        interfaces.KnowledgeBaseService
	kbShareService   interfaces.KBShareService
	knowledgeService interfaces.KnowledgeService
}

// NewExternalHandler creates a new ExternalHandler (wired by dig).
func NewExternalHandler(
	userService interfaces.UserService,
	tenantService interfaces.TenantService,
	kbService interfaces.KnowledgeBaseService,
	kbShareService interfaces.KBShareService,
	knowledgeService interfaces.KnowledgeService,
) *ExternalHandler {
	return &ExternalHandler{
		userService:      userService,
		tenantService:    tenantService,
		kbService:        kbService,
		kbShareService:   kbShareService,
		knowledgeService: knowledgeService,
	}
}

// resolveExternalUser reads X-External-User-Email, looks up the real User, and
// overwrites gin context + request context so downstream handlers see the resolved
// user instead of the API-key synthetic system user.
//
// Returns (user, tenant, ok). When ok==false, the response is already written —
// the caller must return immediately.
// When X-External-User-Email is absent the existing context user is kept (ok==true).
func (h *ExternalHandler) resolveExternalUser(c *gin.Context) (*types.User, *types.Tenant, bool) {
	email := strings.TrimSpace(c.GetHeader("X-External-User-Email"))
	if email == "" {
		u, _ := c.Get(types.UserContextKey.String())
		t, _ := c.Get(types.TenantInfoContextKey.String())
		user, _ := u.(*types.User)
		tenant, _ := t.(*types.Tenant)
		return user, tenant, true
	}

	user, err := h.userService.GetUserByEmail(c.Request.Context(), email)
	if err != nil || user == nil {
		logger.Warnf(c.Request.Context(), "[External] user not found email=%s err=%v", email, err)
		c.JSON(http.StatusForbidden, gin.H{"error": "X-External-User-Email not found"})
		c.Abort()
		return nil, nil, false
	}
	if !user.IsActive {
		c.JSON(http.StatusForbidden, gin.H{"error": "user account is disabled"})
		c.Abort()
		return nil, nil, false
	}

	tenant, err := h.tenantService.GetTenantByID(c.Request.Context(), user.TenantID)
	if err != nil || tenant == nil {
		logger.Warnf(c.Request.Context(), "[External] tenant not found user=%s err=%v", user.ID, err)
		c.JSON(http.StatusForbidden, gin.H{"error": "tenant not found for external user"})
		c.Abort()
		return nil, nil, false
	}

	// Overwrite gin context keys.
	c.Set(types.TenantIDContextKey.String(), user.TenantID)
	c.Set(types.TenantInfoContextKey.String(), tenant)
	c.Set(types.UserContextKey.String(), user)
	c.Set(types.UserIDContextKey.String(), user.ID)

	// Overwrite request context for service-layer code that reads ctx directly.
	ctx := context.WithValue(
		context.WithValue(
			context.WithValue(
				context.WithValue(c.Request.Context(),
					types.TenantIDContextKey, user.TenantID),
				types.TenantInfoContextKey, tenant),
			types.UserContextKey, user),
		types.UserIDContextKey, user.ID,
	)
	c.Request = c.Request.WithContext(ctx)
	return user, tenant, true
}

// ─── Route Handlers ──────────────────────────────────────────────────────────

// ProvisionUser ensures a user with the given email exists in WeKnora (idempotent).
// This is used for bulk pre-provisioning; day-to-day user creation happens
// automatically on first LDAP login.
func (h *ExternalHandler) ProvisionUser(c *gin.Context) {
	var req struct {
		Email    string `json:"email"    binding:"required,email"`
		Username string `json:"username" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(apperrors.NewBadRequestError(err.Error()))
		return
	}
	ctx := c.Request.Context()

	existing, _ := h.userService.GetUserByEmail(ctx, req.Email)
	if existing != nil {
		c.JSON(http.StatusOK, gin.H{"success": true, "data": gin.H{
			"user_id": existing.ID,
			"existed": true,
		}})
		return
	}

	pass, err := generateRandomPasswordHex()
	if err != nil {
		c.Error(apperrors.NewInternalServerError("failed to generate provisional password"))
		return
	}
	user, err := h.userService.Register(ctx, &types.RegisterRequest{
		Email:    req.Email,
		Username: req.Username,
		Password: pass,
	})
	if err != nil {
		logger.Errorf(ctx, "[External] provision user failed email=%s: %v", req.Email, err)
		c.Error(apperrors.NewInternalServerError("provision user failed: " + err.Error()))
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": gin.H{
		"user_id": user.ID,
		"existed": false,
	}})
}

// GetAccessibleKnowledgeBases returns all knowledge bases the resolved user can
// access (own + shared via organizations).
func (h *ExternalHandler) GetAccessibleKnowledgeBases(c *gin.Context) {
	user, _, ok := h.resolveExternalUser(c)
	if !ok {
		return
	}
	ctx := c.Request.Context()

	ownKBs, err := h.kbService.ListKnowledgeBases(ctx)
	if err != nil {
		logger.Errorf(ctx, "[External] list own KBs user=%s: %v", user.ID, err)
		c.Error(apperrors.NewInternalServerError(err.Error()))
		return
	}

	type kbItem struct {
		ID          string `json:"id"`
		Name        string `json:"name"`
		Description string `json:"description"`
		Source      string `json:"source"`
		Permission  string `json:"permission"`
		OrgID       string `json:"org_id,omitempty"`
		OrgName     string `json:"org_name,omitempty"`
	}

	result := make([]kbItem, 0, len(ownKBs))
	for _, kb := range ownKBs {
		result = append(result, kbItem{
			ID:          kb.ID,
			Name:        kb.Name,
			Description: kb.Description,
			Source:      "own",
			Permission:  "admin",
		})
	}

	tenantID := c.GetUint64(types.TenantIDContextKey.String())
	sharedKBs, err := h.kbShareService.ListSharedKnowledgeBases(ctx, user.ID, tenantID)
	if err != nil {
		logger.Warnf(ctx, "[External] list shared KBs user=%s: %v", user.ID, err)
	} else {
		for _, s := range sharedKBs {
			if s.KnowledgeBase == nil {
				continue
			}
			perm := "viewer"
			if s.Permission != "" {
				perm = string(s.Permission)
			}
			result = append(result, kbItem{
				ID:          s.KnowledgeBase.ID,
				Name:        s.KnowledgeBase.Name,
				Description: s.KnowledgeBase.Description,
				Source:      "shared",
				Permission:  perm,
				OrgID:       s.OrganizationID,
				OrgName:     s.OrgName,
			})
		}
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "data": result})
}

// CreateKnowledgeBaseForUser creates a KB scoped to the resolved user's tenant.
func (h *ExternalHandler) CreateKnowledgeBaseForUser(c *gin.Context) {
	var req struct {
		Name        string `json:"name"        binding:"required"`
		Description string `json:"description"`
		Type        string `json:"type"`
	}
	_, _, ok := h.resolveExternalUser(c)
	if !ok {
		return
	}
	ctx := c.Request.Context()

	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(apperrors.NewBadRequestError(err.Error()))
		return
	}
	kbType := req.Type
	if kbType == "" {
		kbType = "document"
	}
	tenantID := c.GetUint64(types.TenantIDContextKey.String())
	kb := &types.KnowledgeBase{
		Name:        req.Name,
		Description: req.Description,
		Type:        kbType,
		TenantID:    tenantID,
	}
	created, err := h.kbService.CreateKnowledgeBase(ctx, kb)
	if err != nil {
		logger.Errorf(ctx, "[External] create KB failed: %v", err)
		c.Error(apperrors.NewInternalServerError(err.Error()))
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": created})
}

// MultiSearch searches across one or more knowledge bases. Optionally restricts
// results to a set of knowledge_ids (AI pool isolation pattern).
func (h *ExternalHandler) MultiSearch(c *gin.Context) {
	var req struct {
		KBIDs          []string `json:"kb_ids"          binding:"required"`
		Query          string   `json:"query"           binding:"required"`
		TopK           int      `json:"top_k"`
		ScoreThreshold float64  `json:"score_threshold"`
		KnowledgeIDs   []string `json:"knowledge_ids"`
	}
	_, _, ok := h.resolveExternalUser(c)
	if !ok {
		return
	}
	ctx := c.Request.Context()

	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(apperrors.NewBadRequestError(err.Error()))
		return
	}
	if req.TopK <= 0 {
		req.TopK = 10
	}
	if req.ScoreThreshold <= 0 {
		req.ScoreThreshold = 0.3
	}

	type searchHit struct {
		KBID    string  `json:"kb_id"`
		Score   float64 `json:"score"`
		Content string  `json:"content"`
	}

	allHits := make([]searchHit, 0)
	params := types.SearchParams{
		QueryText:      req.Query,
		MatchCount:     req.TopK,
		VectorThreshold: req.ScoreThreshold,
		KnowledgeIDs:   req.KnowledgeIDs,
	}

	for _, kbID := range req.KBIDs {
		results, err := h.kbService.HybridSearch(ctx, kbID, params)
		if err != nil {
			logger.Warnf(ctx, "[External] multi-search KB=%s: %v", kbID, err)
			continue
		}
		for _, r := range results {
			allHits = append(allHits, searchHit{
				KBID:    kbID,
				Score:   r.Score,
				Content: r.Content,
			})
		}
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "data": allHits})
}

// ListEntries lists knowledge entries for a KB, filtered by channel (source) and/or tag_id.
// Pass source=xgimi_llm_wiki to list only AI-generated entries.
func (h *ExternalHandler) ListEntries(c *gin.Context) {
	_, _, ok := h.resolveExternalUser(c)
	if !ok {
		return
	}
	ctx := c.Request.Context()

	kbID := strings.TrimSpace(c.Param("id"))
	if kbID == "" {
		c.Error(apperrors.NewBadRequestError("knowledge base id is required"))
		return
	}

	source := strings.TrimSpace(c.Query("source"))
	tagID := strings.TrimSpace(c.Query("tag_id"))
	page := parseExternalIntQuery(c.Query("page"), 1)
	size := parseExternalIntQuery(c.Query("size"), 50)

	filter := types.KnowledgeListFilter{
		Source: source,
		TagID:  tagID,
	}
	pagination := &types.Pagination{Page: page, PageSize: size}

	result, err := h.knowledgeService.ListPagedKnowledgeByKnowledgeBaseID(ctx, kbID, pagination, filter)
	if err != nil {
		logger.Errorf(ctx, "[External] list entries KB=%s: %v", kbID, err)
		c.Error(apperrors.NewInternalServerError(err.Error()))
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": result})
}

// UpdateEntry updates the title and/or content of a manual knowledge entry.
func (h *ExternalHandler) UpdateEntry(c *gin.Context) {
	var req struct {
		Name    string `json:"name"`
		Content string `json:"content"`
	}
	_, _, ok := h.resolveExternalUser(c)
	if !ok {
		return
	}
	ctx := c.Request.Context()

	kbID := strings.TrimSpace(c.Param("id"))
	entryID := strings.TrimSpace(c.Param("entry_id"))
	if kbID == "" || entryID == "" {
		c.Error(apperrors.NewBadRequestError("kb id and entry id are required"))
		return
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(apperrors.NewBadRequestError(err.Error()))
		return
	}

	payload := &types.ManualKnowledgePayload{
		Title:   req.Name,
		Content: req.Content,
		Channel: types.ChannelXgimiLLMWiki,
	}
	updated, err := h.knowledgeService.UpdateManualKnowledge(ctx, entryID, payload)
	if err != nil {
		logger.Errorf(ctx, "[External] update entry KB=%s entry=%s: %v", kbID, entryID, err)
		c.Error(apperrors.NewInternalServerError(err.Error()))
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": updated})
}

// DeleteEntry deletes a knowledge entry. Intended for AI-generated (xgimi_llm_wiki channel) entries.
func (h *ExternalHandler) DeleteEntry(c *gin.Context) {
	_, _, ok := h.resolveExternalUser(c)
	if !ok {
		return
	}
	ctx := c.Request.Context()

	kbID := strings.TrimSpace(c.Param("id"))
	entryID := strings.TrimSpace(c.Param("entry_id"))
	if kbID == "" || entryID == "" {
		c.Error(apperrors.NewBadRequestError("kb id and entry id are required"))
		return
	}
	if err := h.knowledgeService.DeleteKnowledge(ctx, entryID); err != nil {
		logger.Errorf(ctx, "[External] delete entry KB=%s entry=%s: %v", kbID, entryID, err)
		c.Error(apperrors.NewInternalServerError(err.Error()))
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func parseExternalIntQuery(s string, defaultVal int) int {
	if s == "" {
		return defaultVal
	}
	v, err := strconv.Atoi(s)
	if err != nil || v <= 0 {
		return defaultVal
	}
	return v
}

// generateRandomPasswordHex returns a cryptographically random 24-character hex
// string used as a provisional password for LDAP-provisioned accounts. The user
// never needs to enter this password when LDAP auth is enabled.
func generateRandomPasswordHex() (string, error) {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
