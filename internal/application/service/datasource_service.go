package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/textproto"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/tracing/langfuse"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
)

// DataSourceService implements the DataSourceService interface
type DataSourceService struct {
	dsRepo            interfaces.DataSourceRepository
	syncLogRepo       interfaces.SyncLogRepository
	knowledgeService  interfaces.KnowledgeService
	kbService         interfaces.KnowledgeBaseService
	taskEnqueuer      interfaces.TaskEnqueuer
	connectorRegistry *datasource.ConnectorRegistry
	scheduler         *datasource.Scheduler
	tenantRepo        interfaces.TenantRepository
	tagService        interfaces.KnowledgeTagService
}

// NewDataSourceService creates a new data source service
func NewDataSourceService(
	dsRepo interfaces.DataSourceRepository,
	syncLogRepo interfaces.SyncLogRepository,
	knowledgeService interfaces.KnowledgeService,
	kbService interfaces.KnowledgeBaseService,
	taskEnqueuer interfaces.TaskEnqueuer,
	connectorRegistry *datasource.ConnectorRegistry,
	scheduler *datasource.Scheduler,
	tenantRepo interfaces.TenantRepository,
	tagService interfaces.KnowledgeTagService,
) interfaces.DataSourceService {
	return &DataSourceService{
		dsRepo:            dsRepo,
		syncLogRepo:       syncLogRepo,
		knowledgeService:  knowledgeService,
		kbService:         kbService,
		taskEnqueuer:      taskEnqueuer,
		connectorRegistry: connectorRegistry,
		scheduler:         scheduler,
		tenantRepo:        tenantRepo,
		tagService:        tagService,
	}
}

// CreateDataSource creates a new data source configuration
func (s *DataSourceService) CreateDataSource(ctx context.Context, ds *types.DataSource) (*types.DataSource, error) {
	if ds == nil {
		return nil, datasource.ErrDataSourceInvalid
	}

	// Validate knowledge base exists
	kb, err := s.kbService.GetKnowledgeBaseByID(ctx, ds.KnowledgeBaseID)
	if err != nil || kb == nil {
		return nil, datasource.ErrKnowledgeBaseNotFound
	}
	if kb.TenantID != ds.TenantID {
		return nil, datasource.ErrKnowledgeBaseNotFound
	}

	// Validate connector type
	_, err = s.connectorRegistry.Get(ds.Type)
	if err != nil {
		return nil, err
	}

	// Validate configuration
	if err := s.validateDataSourceConfig(ctx, ds); err != nil {
		return nil, err
	}

	// Create in database
	if err := s.dsRepo.Create(ctx, ds); err != nil {
		logger.Errorf(ctx, "failed to create data source: %v", err)
		return nil, err
	}

	// Register cron schedule if configured
	if ds.SyncSchedule != "" && ds.Status == types.DataSourceStatusActive {
		if err := s.scheduler.AddOrUpdate(ds); err != nil {
			logger.Warnf(ctx, "failed to register cron for ds=%s: %v", ds.ID, err)
		}
	}

	logger.Infof(ctx, "data source created: id=%s type=%s kb=%s", ds.ID, ds.Type, ds.KnowledgeBaseID)
	return ds, nil
}

// GetDataSource retrieves a data source by ID
func (s *DataSourceService) GetDataSource(ctx context.Context, id string) (*types.DataSource, error) {
	ds, err := s.dsRepo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	return ds, nil
}

// ListDataSources lists all data sources for a knowledge base
func (s *DataSourceService) ListDataSources(ctx context.Context, kbID string) ([]*types.DataSource, error) {
	dataSources, err := s.dsRepo.FindByKnowledgeBase(ctx, kbID)
	if err != nil {
		logger.Errorf(ctx, "failed to list data sources: %v", err)
		return nil, err
	}

	// Attach latest sync log to each data source
	for _, ds := range dataSources {
		log, _ := s.syncLogRepo.FindLatest(ctx, ds.ID)
		if log != nil {
			ds.LatestSyncLog = log
		}
	}

	return dataSources, nil
}

// UpdateDataSource updates an existing data source
func (s *DataSourceService) UpdateDataSource(ctx context.Context, ds *types.DataSource) (*types.DataSource, error) {
	if ds == nil || ds.ID == "" {
		return nil, datasource.ErrDataSourceInvalid
	}

	// Verify data source exists
	existing, err := s.dsRepo.FindByID(ctx, ds.ID)
	if err != nil {
		return nil, err
	}

	if ds.KnowledgeBaseID == "" {
		ds.KnowledgeBaseID = existing.KnowledgeBaseID
	}
	if ds.KnowledgeBaseID != existing.KnowledgeBaseID {
		return nil, fmt.Errorf("changing knowledge base is not allowed")
	}

	if ds.TenantID == 0 {
		ds.TenantID = existing.TenantID
	}
	if ds.TenantID != existing.TenantID {
		return nil, datasource.ErrDataSourceInvalid
	}

	// Validate new configuration if changed
	if ds.Type != existing.Type || string(ds.Config) != string(existing.Config) {
		if err := s.validateDataSourceConfig(ctx, ds); err != nil {
			return nil, err
		}
	}

	if err := s.dsRepo.Update(ctx, ds); err != nil {
		logger.Errorf(ctx, "failed to update data source: %v", err)
		return nil, err
	}

	// Update cron schedule
	if err := s.scheduler.AddOrUpdate(ds); err != nil {
		logger.Warnf(ctx, "failed to update cron for ds=%s: %v", ds.ID, err)
	}

	logger.Infof(ctx, "data source updated: id=%s", ds.ID)
	return ds, nil
}

// DeleteDataSource deletes a data source (soft delete)
func (s *DataSourceService) DeleteDataSource(ctx context.Context, id string) error {
	// Verify data source exists
	_, err := s.dsRepo.FindByID(ctx, id)
	if err != nil {
		return err
	}

	if err := s.dsRepo.Delete(ctx, id); err != nil {
		logger.Errorf(ctx, "failed to delete data source: %v", err)
		return err
	}

	// Remove cron schedule
	s.scheduler.Remove(id)

	// Cancel any pending/running sync logs so queued asynq tasks won't retry
	if err := s.syncLogRepo.CancelPendingByDataSource(ctx, id); err != nil {
		logger.Warnf(ctx, "failed to cancel pending sync logs for ds=%s: %v", id, err)
	}

	logger.Infof(ctx, "data source deleted: id=%s", id)
	return nil
}

// ValidateConnection tests the connection to an external data source
func (s *DataSourceService) ValidateConnection(ctx context.Context, dsID string) error {
	ds, err := s.GetDataSource(ctx, dsID)
	if err != nil {
		return err
	}

	// Get connector
	connector, err := s.connectorRegistry.Get(ds.Type)
	if err != nil {
		return err
	}

	// Parse configuration
	config, err := ds.ParseConfig()
	if err != nil {
		return datasource.ErrInvalidConfig
	}

	// Validate connection
	if err := connector.Validate(ctx, config); err != nil {
		// Update data source with error
		ds.Status = types.DataSourceStatusError
		ds.ErrorMessage = err.Error()
		_ = s.dsRepo.Update(ctx, ds)
		return err
	}

	// Clear error if it was previously in error state
	if ds.Status == types.DataSourceStatusError {
		ds.Status = types.DataSourceStatusActive
		ds.ErrorMessage = ""
		_ = s.dsRepo.Update(ctx, ds)
	}

	return nil
}

// ListAvailableResources lists resources available for sync in the external system
func (s *DataSourceService) ListAvailableResources(ctx context.Context, dsID string) ([]types.Resource, error) {
	ds, err := s.GetDataSource(ctx, dsID)
	if err != nil {
		return nil, err
	}

	// Get connector
	connector, err := s.connectorRegistry.Get(ds.Type)
	if err != nil {
		return nil, err
	}

	// Parse configuration
	config, err := ds.ParseConfig()
	if err != nil {
		return nil, datasource.ErrInvalidConfig
	}

	// List resources
	resources, err := connector.ListResources(ctx, config)
	if err != nil {
		logger.Errorf(ctx, "failed to list resources: %v", err)
		return nil, err
	}

	return resources, nil
}

// ManualSync triggers an immediate sync for a data source
func (s *DataSourceService) ManualSync(ctx context.Context, dsID string) (*types.SyncLog, error) {
	ds, err := s.GetDataSource(ctx, dsID)
	if err != nil {
		return nil, err
	}

	if ds.Status != types.DataSourceStatusActive &&
		ds.Status != types.DataSourceStatusError &&
		ds.Status != types.DataSourceStatusPaused {
		return nil, datasource.ErrDataSourceNotActive
	}

	// Create sync log
	syncLog := &types.SyncLog{
		DataSourceID: dsID,
		TenantID:     ds.TenantID,
		Status:       types.SyncLogStatusRunning,
		StartedAt:    time.Now().UTC(),
	}

	if err := s.syncLogRepo.Create(ctx, syncLog); err != nil {
		logger.Errorf(ctx, "failed to create sync log: %v", err)
		return nil, err
	}

	// Enqueue sync task
	payload := &types.DataSourceSyncPayload{
		DataSourceID: dsID,
		TenantID:     ds.TenantID,
		SyncLogID:    syncLog.ID,
		ForceFull:    false,
	}
	langfuse.InjectTracing(ctx, payload)

	payloadJSON, _ := json.Marshal(payload)
	task := asynq.NewTask(types.TypeDataSourceSync, payloadJSON)

	_, err = s.taskEnqueuer.Enqueue(task,
		asynq.Queue("default"),
		asynq.Timeout(types.DataSourceSyncTaskTimeout),
	)
	if err != nil {
		logger.Errorf(ctx, "failed to enqueue sync task: %v", err)
		syncLog.Status = types.SyncLogStatusFailed
		syncLog.FinishedAt = timePtr(time.Now().UTC())
		syncLog.ErrorMessage = err.Error()
		_ = s.syncLogRepo.Update(ctx, syncLog)
		if ds.Status != types.DataSourceStatusPaused {
			ds.Status = types.DataSourceStatusError
		}
		ds.ErrorMessage = fmt.Sprintf("Failed to enqueue sync: %v", err)
		_ = s.dsRepo.Update(ctx, ds)
		return nil, err
	}

	logger.Infof(ctx, "sync task enqueued: ds=%s syncLog=%s", dsID, syncLog.ID)
	return syncLog, nil
}

// PauseDataSource pauses a data source's scheduled syncs
func (s *DataSourceService) PauseDataSource(ctx context.Context, id string) error {
	ds, err := s.GetDataSource(ctx, id)
	if err != nil {
		return err
	}

	ds.Status = types.DataSourceStatusPaused
	if err := s.dsRepo.Update(ctx, ds); err != nil {
		logger.Errorf(ctx, "failed to pause data source: %v", err)
		return err
	}

	// Remove cron schedule
	s.scheduler.Remove(id)

	logger.Infof(ctx, "data source paused: id=%s", id)
	return nil
}

// ResumeDataSource resumes a paused data source
func (s *DataSourceService) ResumeDataSource(ctx context.Context, id string) error {
	ds, err := s.GetDataSource(ctx, id)
	if err != nil {
		return err
	}

	ds.Status = types.DataSourceStatusActive
	if err := s.dsRepo.Update(ctx, ds); err != nil {
		logger.Errorf(ctx, "failed to resume data source: %v", err)
		return err
	}

	// Re-register cron schedule
	if err := s.scheduler.AddOrUpdate(ds); err != nil {
		logger.Warnf(ctx, "failed to re-register cron for ds=%s: %v", ds.ID, err)
	}

	logger.Infof(ctx, "data source resumed: id=%s", id)
	return nil
}

// GetSyncLogs retrieves sync history for a data source
func (s *DataSourceService) GetSyncLogs(ctx context.Context, dsID string, limit int, offset int) ([]*types.SyncLog, error) {
	logs, err := s.syncLogRepo.FindByDataSource(ctx, dsID, limit, offset)
	if err != nil {
		logger.Errorf(ctx, "failed to get sync logs: %v", err)
		return nil, err
	}
	return logs, nil
}

// GetSyncLog retrieves a specific sync log entry
func (s *DataSourceService) GetSyncLog(ctx context.Context, syncLogID string) (*types.SyncLog, error) {
	log, err := s.syncLogRepo.FindByID(ctx, syncLogID)
	if err != nil {
		return nil, err
	}
	return log, nil
}

// ProcessSync handles the actual sync operation (called by asynq task)
func (s *DataSourceService) ProcessSync(ctx context.Context, task *asynq.Task) error {
	var payload types.DataSourceSyncPayload
	if err := json.Unmarshal(task.Payload(), &payload); err != nil {
		logger.Errorf(ctx, "failed to unmarshal sync payload: %v", err)
		return err
	}

	logger.Infof(ctx, "processing data source sync: ds=%s syncLog=%s", payload.DataSourceID, payload.SyncLogID)

	// Get data source
	ds, err := s.GetDataSource(ctx, payload.DataSourceID)
	if err != nil {
		logger.Warnf(ctx, "data source not found (likely deleted), cancelling sync: ds=%s err=%v", payload.DataSourceID, err)
		if syncLog, slErr := s.syncLogRepo.FindByID(ctx, payload.SyncLogID); slErr == nil && syncLog != nil {
			s.markSyncCanceled(syncLog, "data source has been deleted")
		}
		return nil
	}

	// Get sync log
	syncLog, err := s.syncLogRepo.FindByID(ctx, payload.SyncLogID)
	if err != nil {
		logger.Errorf(ctx, "failed to get sync log: %v", err)
		return nil
	}

	wasPaused := ds.Status == types.DataSourceStatusPaused

	// Get connector
	connector, err := s.connectorRegistry.Get(ds.Type)
	if err != nil {
		logger.Errorf(ctx, "connector not found: type=%s", ds.Type)
		s.markSyncFailed(syncLog, ds, wasPaused, fmt.Sprintf("Connector not found: %s", ds.Type))
		return err
	}

	// Parse configuration
	config, err := ds.ParseConfig()
	if err != nil {
		logger.Errorf(ctx, "failed to parse config: %v", err)
		s.markSyncFailed(syncLog, ds, wasPaused, fmt.Sprintf("Invalid configuration: %v", err))
		return err
	}

	forceFull := payload.ForceFull || ds.SyncMode == types.SyncModeFull
	var prevCursor *types.SyncCursor
	if !forceFull {
		prevCursor, _ = ds.ParseSyncCursor()
	}

	// Fetch/ingest can run for hours; do not inherit asynq's task deadline.
	workCtx := syncWorkCtx(ctx)

	ingest, err := s.prepareSyncIngest(workCtx, ds)
	if err != nil {
		logger.Errorf(ctx, "failed to prepare sync ingest: %v", err)
		s.markSyncFailed(syncLog, ds, wasPaused, fmt.Sprintf("Failed to prepare sync: %v", err))
		return err
	}

	var result *types.SyncResult
	var nextCursor *types.SyncCursor
	var syncErr error

	if stream, ok := connector.(datasource.StreamingConnector); ok {
		result, nextCursor, syncErr = s.runStreamingSync(workCtx, stream, config, ds, syncLog, ingest, forceFull, prevCursor)
	} else {
		result, nextCursor, syncErr = s.runBatchSync(workCtx, connector, config, ds, syncLog, ingest, forceFull, prevCursor)
	}

	if syncErr != nil {
		logger.Errorf(ctx, "sync operation failed: %v", syncErr)
		s.markSyncFailed(syncLog, ds, wasPaused, fmt.Sprintf("Sync failed: %v", syncErr))
		return syncErr
	}

	s.finalizeSyncSuccess(syncLog, ds, result, nextCursor, wasPaused)

	logger.Infof(ctx, "data source sync completed: ds=%s created=%d updated=%d deleted=%d",
		payload.DataSourceID, syncLog.ItemsCreated, syncLog.ItemsUpdated, syncLog.ItemsDeleted)

	return nil
}

// ValidateCredentials tests connectivity using raw credentials without persisting anything.
func (s *DataSourceService) ValidateCredentials(ctx context.Context, connectorType string, credentials map[string]interface{}) error {
	connector, err := s.connectorRegistry.Get(connectorType)
	if err != nil {
		return err
	}

	config := &types.DataSourceConfig{
		Type:        connectorType,
		Credentials: credentials,
	}

	if err := connector.Validate(ctx, config); err != nil {
		return err
	}

	return nil
}

// Helper functions

func (s *DataSourceService) validateDataSourceConfig(ctx context.Context, ds *types.DataSource) error {
	connector, err := s.connectorRegistry.Get(ds.Type)
	if err != nil {
		return err
	}

	config, err := ds.ParseConfig()
	if err != nil {
		return datasource.ErrInvalidConfig
	}

	return connector.Validate(ctx, config)
}

// ingestItem writes a single FetchedItem into the knowledge base.
// If a knowledge item with the same external_id already exists, it is deleted first (update = delete + re-create).
//
// Routing logic:
//   - Has Content bytes → CreateKnowledgeFromFile (走完整的文档解析 pipeline)
//   - Has URL only      → CreateKnowledgeFromURL  (让 WeKnora 下载并解析)
//
// Returns (isUpdate, error) — isUpdate is true when an existing item was replaced.
func (s *DataSourceService) ingestItem(ctx context.Context, ds *types.DataSource, item *types.FetchedItem, tagID string) (bool, error) {
	channel := ds.Type // e.g. "feishu", "notion"

	metadata := map[string]string{
		"external_id":        item.ExternalID,
		"source_resource_id": item.SourceResourceID,
		"datasource_id":      ds.ID,
	}
	for k, v := range item.Metadata {
		metadata[k] = v
	}

	// Check if a knowledge item with this external_id already exists → delete it first (update)
	isUpdate := false
	if item.ExternalID != "" {
		repo := s.knowledgeService.GetRepository()
		existing, err := repo.FindByMetadataKey(ctx, ds.TenantID, ds.KnowledgeBaseID, "external_id", item.ExternalID)
		if err != nil {
			logger.Warnf(ctx, "failed to check existing knowledge for external_id=%s: %v", item.ExternalID, err)
			// Non-fatal: proceed with creation (may produce duplicate)
		} else if existing != nil {
			logger.Infof(ctx, "found existing knowledge %s for external_id=%s, deleting for update", existing.ID, item.ExternalID)
			if err := s.knowledgeService.DeleteKnowledge(ctx, existing.ID); err != nil {
				logger.Warnf(ctx, "failed to delete existing knowledge %s: %v", existing.ID, err)
			} else {
				isUpdate = true
			}
		}
	}

	// Case 1: content already fetched → build a FileHeader from bytes and call CreateKnowledgeFromFile
	if len(item.Content) > 0 {
		fh, err := bytesToFileHeader(item.Content, item.FileName)
		if err != nil {
			return isUpdate, fmt.Errorf("build file header: %w", err)
		}
		_, err = s.knowledgeService.CreateKnowledgeFromFile(
			ctx,
			ds.KnowledgeBaseID,
			fh,
			metadata,
			nil,           // use KB default for multimodal
			item.FileName, // customFileName — must include extension for file-type validation
			tagID,         // auto-tag from data source
			channel,
		)
		return isUpdate, err
	}

	// Case 2: only a remote URL — let WeKnora handle downloading and parsing
	if item.URL != "" {
		_, err := s.knowledgeService.CreateKnowledgeFromURL(
			ctx,
			ds.KnowledgeBaseID,
			item.URL,
			item.FileName,
			"",  // auto-detect file type
			nil, // use KB default for multimodal
			item.Title,
			tagID, // auto-tag from data source
			channel,
		)
		return isUpdate, err
	}

	return isUpdate, fmt.Errorf("item has neither content nor URL")
}

// bytesToFileHeader wraps a []byte into a *multipart.FileHeader so it can be
// consumed by KnowledgeService.CreateKnowledgeFromFile.
func bytesToFileHeader(data []byte, filename string) (*multipart.FileHeader, error) {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	// Create a form file part
	partHeader := make(textproto.MIMEHeader)
	partHeader.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, filename))
	partHeader.Set("Content-Type", "application/octet-stream")

	part, err := writer.CreatePart(partHeader)
	if err != nil {
		return nil, fmt.Errorf("create multipart part: %w", err)
	}

	if _, err := part.Write(data); err != nil {
		return nil, fmt.Errorf("write data to part: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close multipart writer: %w", err)
	}

	// Parse the multipart data to get a FileHeader
	reader := multipart.NewReader(&buf, writer.Boundary())
	form, err := reader.ReadForm(int64(len(data)) + 1024)
	if err != nil {
		return nil, fmt.Errorf("read multipart form: %w", err)
	}

	files := form.File["file"]
	if len(files) == 0 {
		return nil, fmt.Errorf("no file in multipart form")
	}

	return files[0], nil
}

func timePtr(t time.Time) *time.Time {
	utc := t.UTC()
	return &utc
}
