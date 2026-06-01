package feishu

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
)

// Connector implements the datasource.Connector interface for Feishu.
type Connector struct{}

// NewConnector creates a new Feishu connector.
func NewConnector() *Connector {
	return &Connector{}
}

// Type returns the connector type identifier.
func (c *Connector) Type() string {
	return types.ConnectorTypeFeishu
}

// Validate verifies that the Feishu configuration is valid by testing connectivity.
func (c *Connector) Validate(ctx context.Context, config *types.DataSourceConfig) error {
	feishuConfig, err := parseFeishuConfig(config)
	if err != nil {
		return err
	}

	client := NewClient(feishuConfig)
	if err := client.Ping(ctx); err != nil {
		return fmt.Errorf("feishu connection failed: %w", err)
	}

	return nil
}

// ListResources lists all accessible Feishu Wiki spaces as selectable resources.
func (c *Connector) ListResources(ctx context.Context, config *types.DataSourceConfig) ([]types.Resource, error) {
	feishuConfig, err := parseFeishuConfig(config)
	if err != nil {
		return nil, err
	}

	client := NewClient(feishuConfig)
	spaces, err := client.ListWikiSpaces(ctx)
	if err != nil {
		return nil, fmt.Errorf("list feishu wiki spaces: %w", err)
	}

	resources := make([]types.Resource, 0, len(spaces))
	for _, space := range spaces {
		resources = append(resources, types.Resource{
			ExternalID:  space.SpaceID,
			Name:        space.Name,
			Type:        "wiki_space",
			Description: space.Description,
			URL:         fmt.Sprintf("https://feishu.cn/wiki/%s", space.SpaceID),
			Metadata: map[string]interface{}{
				"visibility": space.Visibility,
			},
		})
	}

	return resources, nil
}

// FetchAll performs a full sync of all documents from the specified wiki spaces.
func (c *Connector) FetchAll(ctx context.Context, config *types.DataSourceConfig, resourceIDs []string) ([]types.FetchedItem, error) {
	var allItems []types.FetchedItem
	err := c.FetchAllStream(ctx, config, resourceIDs, datasource.StreamCallbacks{
		Emit: func(item types.FetchedItem) error {
			allItems = append(allItems, item)
			return nil
		},
	})
	return allItems, err
}

// FetchIncremental performs an incremental sync by comparing node edit times
// against the previously recorded state.
func (c *Connector) FetchIncremental(ctx context.Context, config *types.DataSourceConfig, cursor *types.SyncCursor) ([]types.FetchedItem, *types.SyncCursor, error) {
	var changedItems []types.FetchedItem
	nextCursor, err := c.FetchIncrementalStream(ctx, config, cursor, datasource.StreamCallbacks{
		Emit: func(item types.FetchedItem) error {
			changedItems = append(changedItems, item)
			return nil
		},
	})
	return changedItems, nextCursor, err
}

func appendWikiNodeListFailureItems(items []types.FetchedItem, spaceID string, failures []wikiNodeListFailure) []types.FetchedItem {
	for _, failure := range failures {
		node := failure.Node
		title := node.Title
		if title == "" {
			title = node.NodeToken
		}
		items = append(items, types.FetchedItem{
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
		})
	}
	return items
}

// fetchNodeContent fetches the content of a single wiki node and converts it to FetchedItem.
// Dispatches to different retrieval strategies based on obj_type:
//   - docx/doc   → export API → .docx file
//   - sheet      → export API → .xlsx file
//   - bitable    → export API → .xlsx file
//   - file       → drive download → original file (PDF/Word/image/etc.)
//   - mindnote   → skip (no API)
//   - slides     → skip (no API)
func (c *Connector) fetchNodeContent(ctx context.Context, client *Client, node wikiNode, spaceID string) (*types.FetchedItem, error) {
	if !isSupportedDocType(node.ObjType) {
		return nil, nil
	}

	editTime := parseFeishuTimestamp(node.NodeEditTime)
	baseMeta := map[string]string{
		"obj_token":  node.ObjToken,
		"obj_type":   node.ObjType,
		"node_token": node.NodeToken,
		"space_id":   spaceID,
		"creator":    node.Creator,
		"owner":      node.Owner,
		"channel":    types.ChannelFeishu,
	}

	switch node.ObjType {
	case "docx", "doc", "sheet", "bitable":
		// Export as a file via the async export API
		data, fileName, err := client.ExportAndDownload(ctx, node.ObjToken, node.ObjType)
		if err != nil {
			return nil, fmt.Errorf("export %s (%s): %w", node.Title, node.ObjType, err)
		}

		// Ensure a reasonable file name with correct extension
		ext := exportFileExtToSuffix[objTypeToExportFileExtension[node.ObjType]]
		if fileName == "" {
			fileName = sanitizeFileName(node.Title) + ext
		} else if !strings.HasSuffix(strings.ToLower(fileName), ext) {
			// Feishu often returns the doc title without extension — append it
			fileName = sanitizeFileName(fileName) + ext
		}

		return &types.FetchedItem{
			ExternalID:       node.NodeToken,
			Title:            node.Title,
			Content:          data,
			ContentType:      "application/octet-stream",
			FileName:         fileName,
			URL:              fmt.Sprintf("https://feishu.cn/wiki/%s", node.NodeToken),
			UpdatedAt:        editTime,
			SourceResourceID: spaceID,
			Metadata:         baseMeta,
		}, nil

	case "file":
		// Download the original uploaded file from Drive
		data, err := client.DownloadDriveFile(ctx, node.ObjToken)
		if err != nil {
			return nil, fmt.Errorf("download file %s (%s): %w", node.Title, node.ObjToken, err)
		}

		// Use the node title as file name; it usually preserves the original extension
		fileName := node.Title
		if fileName == "" {
			fileName = node.ObjToken
		}

		return &types.FetchedItem{
			ExternalID:       node.NodeToken,
			Title:            node.Title,
			Content:          data,
			ContentType:      "application/octet-stream",
			FileName:         fileName,
			URL:              fmt.Sprintf("https://feishu.cn/wiki/%s", node.NodeToken),
			UpdatedAt:        editTime,
			SourceResourceID: spaceID,
			Metadata:         baseMeta,
		}, nil

	default:
		return nil, nil
	}
}

// --- Helper functions ---

// parseFeishuConfig extracts and validates Feishu-specific configuration.
func parseFeishuConfig(config *types.DataSourceConfig) (*Config, error) {
	if config == nil {
		return nil, fmt.Errorf("config is nil")
	}

	credBytes, err := json.Marshal(config.Credentials)
	if err != nil {
		return nil, fmt.Errorf("marshal credentials: %w", err)
	}

	var feishuConfig Config
	if err := json.Unmarshal(credBytes, &feishuConfig); err != nil {
		return nil, fmt.Errorf("parse feishu credentials: %w", err)
	}

	if feishuConfig.AppID == "" || feishuConfig.AppSecret == "" {
		return nil, fmt.Errorf("feishu app_id and app_secret are required")
	}

	return &feishuConfig, nil
}

// isSupportedDocType checks if a Feishu document type can be synced.
// mindnote and slides have no content read API and are skipped.
func isSupportedDocType(objType string) bool {
	switch objType {
	case "docx", "doc", "sheet", "bitable", "file":
		return true
	default:
		// mindnote, slides — no content retrieval API available
		return false
	}
}

// parseFeishuTimestamp parses a Feishu unix timestamp string (seconds) into time.Time.
func parseFeishuTimestamp(ts string) time.Time {
	if ts == "" {
		return time.Time{}
	}
	sec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.Unix(sec, 0)
}

// sanitizeFileName removes characters that are invalid in filenames and
// truncates at a UTF-8 rune boundary. Raw byte truncation would split a
// multi-byte codepoint (Chinese chars are 3 bytes) and produce invalid UTF-8
// that downstream validation (utf8.ValidString) rejects.
func sanitizeFileName(name string) string {
	if name == "" {
		return "untitled"
	}
	replacer := strings.NewReplacer(
		"/", "_", "\\", "_", ":", "_", "*", "_",
		"?", "_", "\"", "_", "<", "_", ">", "_", "|", "_",
	)
	result := replacer.Replace(name)
	const maxBytes = 200
	if len(result) > maxBytes {
		result = result[:maxBytes]
		for len(result) > 0 {
			r, size := utf8.DecodeLastRuneInString(result)
			if r != utf8.RuneError || size != 1 {
				break
			}
			result = result[:len(result)-1]
		}
	}
	return result
}
