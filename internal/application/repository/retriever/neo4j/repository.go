package neo4j

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
)

// Neo4jRepository is a repository for Neo4j
type Neo4jRepository struct {
	driver     neo4j.Driver
	nodePrefix string
}

// NewNeo4jRepository creates a new Neo4j repository
func NewNeo4jRepository(driver neo4j.Driver) interfaces.RetrieveGraphRepository {
	return &Neo4jRepository{driver: driver, nodePrefix: "ENTITY"}
}

// _remove_hyphen removes hyphens from a string
func _remove_hyphen(s string) string {
	return strings.ReplaceAll(s, "-", "_")
}

// safeRelType limits relationship types to Cypher-safe identifiers (GDB has no APOC merge.relationship).
var safeRelType = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func sanitizeRelType(relType string) (string, error) {
	relType = strings.TrimSpace(relType)
	if relType == "" || !safeRelType.MatchString(relType) {
		return "", fmt.Errorf("invalid relationship type: %q", relType)
	}
	return relType, nil
}

// Labels returns the labels for a namespace
func (n *Neo4jRepository) Labels(namespace types.NameSpace) []string {
	res := make([]string, 0)
	for _, label := range namespace.Labels() {
		res = append(res, n.nodePrefix+_remove_hyphen(label))
	}
	return res
}

// Label returns the label for a namespace
func (n *Neo4jRepository) Label(namespace types.NameSpace) string {
	labels := n.Labels(namespace)
	return strings.Join(labels, ":")
}

// AddGraph adds a graph to the Neo4j repository
func (n *Neo4jRepository) AddGraph(ctx context.Context, namespace types.NameSpace, graphs []*types.GraphData) error {
	if n.driver == nil {
		logger.Warnf(ctx, "NOT SUPPORT RETRIEVE GRAPH")
		return nil
	}
	for _, graph := range graphs {
		if err := n.addGraph(ctx, namespace, graph); err != nil {
			return err
		}
	}
	return nil
}

// addGraph adds a graph to the Neo4j repository
func (n *Neo4jRepository) addGraph(ctx context.Context, namespace types.NameSpace, graph *types.GraphData) error {
	session := n.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	labelExpr := n.Label(namespace)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		// 标准 Cypher MERGE：兼容阿里云 GDB（不支持 APOC merge.node / merge.relationship）
		nodeImportQuery := `
			UNWIND $data AS row
			MERGE (node:` + labelExpr + ` {name: row.name, kg: $knowledge_id})
			SET node.attributes = row.attributes,
			    node.chunks = CASE
			        WHEN node.chunks IS NULL THEN row.chunks
			        ELSE [c IN node.chunks WHERE NOT c IN row.chunks] + row.chunks
			    END
		`
		nodeData := make([]map[string]interface{}, 0, len(graph.Node))
		for _, node := range graph.Node {
			nodeData = append(nodeData, map[string]interface{}{
				"name":       node.Name,
				"attributes": node.Attributes,
				"chunks":     node.Chunks,
			})
		}
		if len(nodeData) > 0 {
			if _, err := tx.Run(ctx, nodeImportQuery, map[string]interface{}{
				"data":          nodeData,
				"knowledge_id":  namespace.Knowledge,
			}); err != nil {
				return nil, fmt.Errorf("failed to create nodes: %v", err)
			}
		}

		for _, rel := range graph.Relation {
			relType, err := sanitizeRelType(rel.Type)
			if err != nil {
				return nil, fmt.Errorf("failed to create relationships: %w", err)
			}
			relImportQuery := fmt.Sprintf(`
				MATCH (source:%s {name: $source, kg: $knowledge_id})
				MATCH (target:%s {name: $target, kg: $knowledge_id})
				MERGE (source)-[r:%s]->(target)
			`, labelExpr, labelExpr, relType)
			if _, err := tx.Run(ctx, relImportQuery, map[string]interface{}{
				"source":       rel.Node1,
				"target":       rel.Node2,
				"knowledge_id": namespace.Knowledge,
			}); err != nil {
				return nil, fmt.Errorf("failed to create relationships: %v", err)
			}
		}
		return nil, nil
	})
	if err != nil {
		logger.Errorf(ctx, "failed to add graph: %v", err)
		return err
	}
	return nil
}

// DelGraph deletes a graph from the Neo4j repository
func (n *Neo4jRepository) DelGraph(ctx context.Context, namespaces []types.NameSpace) error {
	if n.driver == nil {
		logger.Warnf(ctx, "NOT SUPPORT RETRIEVE GRAPH")
		return nil
	}
	session := n.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	result, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		for _, namespace := range namespaces {
			labelExpr := n.Label(namespace)
			// 标准 Cypher：兼容阿里云 GDB（不支持 APOC / apoc.periodic.iterate）
			deleteQuery := `
				MATCH (n:` + labelExpr + ` {kg: $knowledge_id})
				DETACH DELETE n
			`
			if _, err := tx.Run(ctx, deleteQuery, map[string]interface{}{"knowledge_id": namespace.Knowledge}); err != nil {
				return nil, fmt.Errorf("failed to delete graph: %v", err)
			}
		}
		return nil, nil
	})
	if err != nil {
		return err
	}
	logger.Infof(ctx, "delete graph result: %v", result)
	return nil
}

// SearchNode searches for nodes in the Neo4j repository
func (n *Neo4jRepository) SearchNode(
	ctx context.Context,
	namespace types.NameSpace,
	nodes []string,
) (*types.GraphData, error) {
	if n.driver == nil {
		logger.Warnf(ctx, "NOT SUPPORT RETRIEVE GRAPH")
		return nil, nil
	}
	session := n.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	result, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		labelExpr := n.Label(namespace)
		query := `
			MATCH (n:` + labelExpr + `)-[r]-(m:` + labelExpr + `)
			WHERE ANY(nodeText IN $nodes WHERE n.name CONTAINS nodeText)
			RETURN n, r, m
		`
		params := map[string]interface{}{"nodes": nodes}
		result, err := tx.Run(ctx, query, params)
		if err != nil {
			return nil, fmt.Errorf("failed to run query: %v", err)
		}

		graphData := &types.GraphData{}
		nodeSeen := make(map[string]bool)
		for result.Next(ctx) {
			record := result.Record()
			node, _ := record.Get("n")
			rel, _ := record.Get("r")
			targetNode, _ := record.Get("m")

			nodeData := node.(neo4j.Node)
			targetNodeData := targetNode.(neo4j.Node)

			// Convert node to types.Node
			for _, n := range []neo4j.Node{nodeData, targetNodeData} {
				nameStr := n.Props["name"].(string)
				if _, ok := nodeSeen[nameStr]; !ok {
					nodeSeen[nameStr] = true
					graphData.Node = append(graphData.Node, &types.GraphNode{
						Name:       nameStr,
						Chunks:     listI2listS(n.Props["chunks"].([]interface{})),
						Attributes: listI2listS(n.Props["attributes"].([]interface{})),
					})
				}
			}

			// Convert relationship to types.Relation
			relData := rel.(neo4j.Relationship)
			graphData.Relation = append(graphData.Relation, &types.GraphRelation{
				Node1: nodeData.Props["name"].(string),
				Node2: targetNodeData.Props["name"].(string),
				Type:  relData.Type,
			})
		}
		return graphData, nil
	})
	if err != nil {
		logger.Errorf(ctx, "search node failed: %v", err)
		return nil, err
	}
	return result.(*types.GraphData), nil
}

func listI2listS(list []any) []string {
	result := make([]string, len(list))
	for i, v := range list {
		result[i] = fmt.Sprintf("%v", v)
	}
	return result
}
