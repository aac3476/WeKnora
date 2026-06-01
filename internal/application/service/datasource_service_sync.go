package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
)

const syncProgressFlushInterval = 5

// syncPersistCtx returns a context for DB writes that must succeed even when the
// asynq task context has been cancelled.
func syncPersistCtx() context.Context {
	return context.Background()
}

// syncWorkCtx is used for remote fetch and per-item ingest during ProcessSync.
// It does not inherit asynq's per-task deadline and is not cancelled when the
// parent hits DeadlineExceeded, so long-running connector fetches can finish.
func syncWorkCtx(parent context.Context) context.Context {
	return context.WithoutCancel(parent)
}

type syncIngestContext struct {
	ctx       context.Context
	ds        *types.DataSource
	autoTagID string
}

func (s *DataSourceService) prepareSyncIngest(ctx context.Context, ds *types.DataSource) (*syncIngestContext, error) {
	ctx = context.WithValue(ctx, types.TenantIDContextKey, ds.TenantID)

	tenant, err := s.tenantRepo.GetTenantByID(ctx, ds.TenantID)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, tenant)

	autoTagID := ""
	autoTagName := ds.Name
	if autoTag, tagErr := s.tagService.FindOrCreateTagByName(ctx, ds.KnowledgeBaseID, autoTagName); tagErr != nil {
		logger.Warnf(ctx, "failed to find/create auto-tag %q: %v (proceeding without tag)", autoTagName, tagErr)
	} else if autoTag != nil {
		autoTagID = autoTag.ID
		logger.Infof(ctx, "using auto-tag %q (id=%s) for data source sync", autoTagName, autoTagID)
	}

	return &syncIngestContext{ctx: ctx, ds: ds, autoTagID: autoTagID}, nil
}

func (s *DataSourceService) processSyncItem(
	ingest *syncIngestContext,
	ds *types.DataSource,
	item *types.FetchedItem,
	result *types.SyncResult,
) error {
	if item.IsDeleted {
		if ds.SyncDeletions {
			result.Deleted++
		}
		return nil
	}

	if len(item.Content) == 0 && item.URL == "" {
		if errMsg, hasErr := item.Metadata["error"]; hasErr {
			logger.Warnf(ingest.ctx, "item %q (external_id=%s) fetch failed: %s", item.Title, item.ExternalID, errMsg)
			result.Failed++
			result.Errors = append(result.Errors, fmt.Sprintf("%s: %s", item.Title, errMsg))
		} else {
			logger.Infof(ingest.ctx, "skipping item %q (external_id=%s): no content or URL", item.Title, item.ExternalID)
			result.Skipped++
		}
		return nil
	}

	isUpdate, err := s.ingestItem(ingest.ctx, ds, item, ingest.autoTagID)
	if err != nil {
		var dupErr *types.DuplicateKnowledgeError
		if errors.As(err, &dupErr) {
			logger.Infof(ingest.ctx, "item %q (external_id=%s) already exists, skipping", item.Title, item.ExternalID)
			result.Skipped++
		} else {
			logger.Warnf(ingest.ctx, "failed to ingest item %q (external_id=%s): %v", item.Title, item.ExternalID, err)
			result.Failed++
			result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", item.Title, err))
		}
		return nil
	}
	if isUpdate {
		result.Updated++
	} else {
		result.Created++
	}
	return nil
}

func (s *DataSourceService) persistSyncLog(syncLog *types.SyncLog) {
	if err := s.syncLogRepo.Update(syncPersistCtx(), syncLog); err != nil {
		logger.Errorf(syncPersistCtx(), "failed to persist sync log: %v", err)
	}
}

func (s *DataSourceService) persistDataSource(ds *types.DataSource) {
	if err := s.dsRepo.Update(syncPersistCtx(), ds); err != nil {
		logger.Errorf(syncPersistCtx(), "failed to persist data source: %v", err)
	}
}

func (s *DataSourceService) persistSyncProgress(syncLog *types.SyncLog, result *types.SyncResult, plannedTotal int) {
	syncLog.ItemsTotal = plannedTotal
	syncLog.ItemsCreated = result.Created
	syncLog.ItemsUpdated = result.Updated
	syncLog.ItemsDeleted = result.Deleted
	syncLog.ItemsSkipped = result.Skipped
	syncLog.ItemsFailed = result.Failed
	s.persistSyncLog(syncLog)
}

func (s *DataSourceService) markSyncCanceled(syncLog *types.SyncLog, message string) {
	syncLog.Status = types.SyncLogStatusCanceled
	syncLog.FinishedAt = timePtr(time.Now().UTC())
	syncLog.ErrorMessage = message
	s.persistSyncLog(syncLog)
}

func (s *DataSourceService) markSyncFailed(
	syncLog *types.SyncLog,
	ds *types.DataSource,
	wasPaused bool,
	message string,
) {
	syncLog.Status = types.SyncLogStatusFailed
	syncLog.FinishedAt = timePtr(time.Now().UTC())
	syncLog.ErrorMessage = message
	s.persistSyncLog(syncLog)
	if !wasPaused {
		ds.Status = types.DataSourceStatusError
	}
	ds.ErrorMessage = message
	s.persistDataSource(ds)
}

func (s *DataSourceService) finalizeSyncSuccess(
	syncLog *types.SyncLog,
	ds *types.DataSource,
	result *types.SyncResult,
	nextCursor *types.SyncCursor,
	wasPaused bool,
) {
	syncLog.ItemsTotal = result.Total
	syncLog.ItemsCreated = result.Created
	syncLog.ItemsUpdated = result.Updated
	syncLog.ItemsDeleted = result.Deleted
	syncLog.ItemsSkipped = result.Skipped
	syncLog.ItemsFailed = result.Failed
	syncLog.Status = types.SyncLogStatusSuccess
	syncLog.FinishedAt = timePtr(time.Now().UTC())

	if nextCursor != nil {
		cursorJSON, _ := nextCursor.ToJSON()
		ds.LastSyncCursor = cursorJSON
	}

	ds.LastSyncAt = timePtr(time.Now().UTC())
	if wasPaused {
		ds.Status = types.DataSourceStatusPaused
	} else {
		ds.Status = types.DataSourceStatusActive
	}
	ds.ErrorMessage = ""

	resultJSON, _ := result.ToJSON()
	ds.LastSyncResult = resultJSON
	syncLog.Result = resultJSON

	s.persistDataSource(ds)
	s.persistSyncLog(syncLog)
}

func (s *DataSourceService) runStreamingSync(
	ctx context.Context,
	stream datasource.StreamingConnector,
	config *types.DataSourceConfig,
	ds *types.DataSource,
	syncLog *types.SyncLog,
	ingest *syncIngestContext,
	forceFull bool,
	prevCursor *types.SyncCursor,
) (*types.SyncResult, *types.SyncCursor, error) {
	result := &types.SyncResult{}
	plannedTotal := 0
	processed := 0

	emit := func(item types.FetchedItem) error {
		processed++
		if err := s.processSyncItem(ingest, ds, &item, result); err != nil {
			return err
		}
		if processed%syncProgressFlushInterval == 0 || processed == plannedTotal {
			s.persistSyncProgress(syncLog, result, plannedTotal)
		}
		return nil
	}

	cb := datasource.StreamCallbacks{
		OnDiscovered: func(delta int) {
			plannedTotal += delta
			result.Total = plannedTotal
			s.persistSyncProgress(syncLog, result, plannedTotal)
		},
		Emit: emit,
	}

	var nextCursor *types.SyncCursor
	var fetchErr error

	if forceFull {
		logger.Infof(ctx, "starting streaming full sync for ds=%s", ds.ID)
		fetchErr = stream.FetchAllStream(ctx, config, config.ResourceIDs, cb)
	} else {
		logger.Infof(ctx, "starting streaming incremental sync for ds=%s", ds.ID)
		nextCursor, fetchErr = stream.FetchIncrementalStream(ctx, config, prevCursor, cb)
	}

	if fetchErr != nil {
		return result, nextCursor, fetchErr
	}

	// Final progress flush
	result.Total = plannedTotal
	s.persistSyncProgress(syncLog, result, plannedTotal)
	logger.Infof(ctx, "streaming sync pass done: planned=%d created=%d updated=%d failed=%d",
		plannedTotal, result.Created, result.Updated, result.Failed)

	return result, nextCursor, nil
}

func (s *DataSourceService) runBatchSync(
	ctx context.Context,
	connector datasource.Connector,
	config *types.DataSourceConfig,
	ds *types.DataSource,
	syncLog *types.SyncLog,
	ingest *syncIngestContext,
	forceFull bool,
	prevCursor *types.SyncCursor,
) (*types.SyncResult, *types.SyncCursor, error) {
	var items []types.FetchedItem
	var nextCursor *types.SyncCursor
	var fetchErr error

	if forceFull {
		items, fetchErr = connector.FetchAll(ctx, config, config.ResourceIDs)
		logger.Infof(ctx, "full sync fetched %d items", len(items))
	} else {
		items, nextCursor, fetchErr = connector.FetchIncremental(ctx, config, prevCursor)
		logger.Infof(ctx, "incremental sync fetched %d items", len(items))
	}
	if fetchErr != nil {
		return nil, nextCursor, fetchErr
	}

	result := &types.SyncResult{Total: len(items)}
	s.persistSyncProgress(syncLog, result, len(items))

	for i, item := range items {
		if err := s.processSyncItem(ingest, ds, &item, result); err != nil {
			return result, nextCursor, err
		}
		if (i+1)%syncProgressFlushInterval == 0 || i+1 == len(items) {
			s.persistSyncProgress(syncLog, result, len(items))
		}
	}

	return result, nextCursor, nil
}
