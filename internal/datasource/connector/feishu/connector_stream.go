package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
)

// FetchAllStream lists wiki nodes and fetches each document, invoking cb.Emit per item.
func (c *Connector) FetchAllStream(
	ctx context.Context,
	config *types.DataSourceConfig,
	resourceIDs []string,
	cb datasource.StreamCallbacks,
) error {
	feishuConfig, err := parseFeishuConfig(config)
	if err != nil {
		return err
	}

	client := NewClient(feishuConfig)

	for _, spaceID := range resourceIDs {
		nodes, err := client.ListAllWikiNodesRecursive(ctx, spaceID)
		if err != nil {
			var partialErr *partialWikiNodeListError
			if !errors.As(err, &partialErr) {
				return fmt.Errorf("list nodes in space %s: %w", spaceID, err)
			}
			if cb.OnDiscovered != nil {
				cb.OnDiscovered(len(partialErr.Failures))
			}
			if err := emitWikiNodeListFailures(cb, spaceID, partialErr.Failures); err != nil {
				return err
			}
		}

		if cb.OnDiscovered != nil {
			cb.OnDiscovered(len(nodes))
		}

		for _, node := range nodes {
			if err := c.emitNodeContent(ctx, client, node, spaceID, cb.Emit); err != nil {
				return err
			}
		}
	}

	return nil
}

// FetchIncrementalStream performs incremental sync, emitting changed or deleted items.
func (c *Connector) FetchIncrementalStream(
	ctx context.Context,
	config *types.DataSourceConfig,
	cursor *types.SyncCursor,
	cb datasource.StreamCallbacks,
) (*types.SyncCursor, error) {
	feishuConfig, err := parseFeishuConfig(config)
	if err != nil {
		return nil, err
	}

	client := NewClient(feishuConfig)

	var prevCursor feishuCursor
	if cursor != nil && cursor.ConnectorCursor != nil {
		cursorBytes, _ := json.Marshal(cursor.ConnectorCursor)
		_ = json.Unmarshal(cursorBytes, &prevCursor)
	}

	newCursor := feishuCursor{
		LastSyncTime:   time.Now(),
		SpaceNodeTimes: make(map[string]map[string]string),
	}

	resourceIDs := config.ResourceIDs
	if len(resourceIDs) == 0 {
		return nil, fmt.Errorf("no resource IDs (wiki space IDs) configured")
	}

	for _, spaceID := range resourceIDs {
		nodes, err := client.ListAllWikiNodesRecursive(ctx, spaceID)
		var partialErr *partialWikiNodeListError
		if err != nil {
			if !errors.As(err, &partialErr) {
				return nil, fmt.Errorf("list nodes in space %s: %w", spaceID, err)
			}
			if cb.OnDiscovered != nil {
				cb.OnDiscovered(len(partialErr.Failures))
			}
			if err := emitWikiNodeListFailures(cb, spaceID, partialErr.Failures); err != nil {
				return nil, err
			}
		}

		newCursor.SpaceNodeTimes[spaceID] = make(map[string]string)
		if partialErr != nil && prevCursor.SpaceNodeTimes != nil {
			if prevTimes, ok := prevCursor.SpaceNodeTimes[spaceID]; ok {
				for nodeToken, editTime := range prevTimes {
					newCursor.SpaceNodeTimes[spaceID][nodeToken] = editTime
				}
			}
		}

		currentNodes := make(map[string]bool)

		for _, node := range nodes {
			currentNodes[node.NodeToken] = true
			editTimeStr := node.ObjEditTime
			if editTimeStr == "" {
				editTimeStr = node.NodeEditTime
			}
			newCursor.SpaceNodeTimes[spaceID][node.NodeToken] = editTimeStr

			if prevCursor.SpaceNodeTimes != nil {
				if prevTimes, ok := prevCursor.SpaceNodeTimes[spaceID]; ok {
					if prevEditTime, exists := prevTimes[node.NodeToken]; exists && prevEditTime == editTimeStr {
						continue
					}
				}
			}

			if cb.OnDiscovered != nil {
				cb.OnDiscovered(1)
			}
			if err := c.emitNodeContent(ctx, client, node, spaceID, cb.Emit); err != nil {
				return nil, err
			}
		}

		if partialErr == nil && prevCursor.SpaceNodeTimes != nil {
			if prevTimes, ok := prevCursor.SpaceNodeTimes[spaceID]; ok {
				for nodeToken := range prevTimes {
					if currentNodes[nodeToken] {
						continue
					}
					if cb.OnDiscovered != nil {
						cb.OnDiscovered(1)
					}
					if cb.Emit == nil {
						continue
					}
					if err := cb.Emit(types.FetchedItem{
						ExternalID:       nodeToken,
						IsDeleted:        true,
						SourceResourceID: spaceID,
					}); err != nil {
						return nil, err
					}
				}
			}
		}
	}

	nextCursorMap := make(map[string]interface{})
	cursorBytes, _ := json.Marshal(newCursor)
	_ = json.Unmarshal(cursorBytes, &nextCursorMap)

	return &types.SyncCursor{
		LastSyncTime:    time.Now(),
		ConnectorCursor: nextCursorMap,
	}, nil
}

func emitWikiNodeListFailures(cb datasource.StreamCallbacks, spaceID string, failures []wikiNodeListFailure) error {
	if cb.Emit == nil {
		return nil
	}
	for _, failure := range failures {
		node := failure.Node
		title := node.Title
		if title == "" {
			title = node.NodeToken
		}
		if err := cb.Emit(types.FetchedItem{
			ExternalID:       node.NodeToken,
			Title:            title,
			SourceResourceID: spaceID,
			Metadata: map[string]string{
				"error":         failure.Err.Error(),
				"channel":       types.ChannelFeishu,
				"node_token":    node.NodeToken,
				"space_id":      spaceID,
				"failure_stage": "list_children",
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

func (c *Connector) emitNodeContent(
	ctx context.Context,
	client *Client,
	node wikiNode,
	spaceID string,
	emit datasource.SyncEmitFunc,
) error {
	if emit == nil {
		return nil
	}

	item, err := c.fetchNodeContent(ctx, client, node, spaceID)
	if err != nil {
		return emit(types.FetchedItem{
			ExternalID:       node.NodeToken,
			Title:            node.Title,
			SourceResourceID: spaceID,
			Metadata: map[string]string{
				"error": err.Error(),
			},
		})
	}
	if item != nil {
		return emit(*item)
	}
	return nil
}
