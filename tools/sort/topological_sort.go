package sort

import (
	"errors"
	"fmt"
)

// NodeState represents the state of a node during DFS traversal
type NodeState int

const (
	UNVISITED NodeState = 0 // unvisited
	VISITING  NodeState = 1 // visiting (in the DFS stack)
	VISITED   NodeState = 2 // visited
)

// CycleError means that a cycle was detected in the graph during topological sorting
var CycleError = errors.New("graph contains a cycle")

// TopologicalSortReachable sorts the nodes reachable from startNodeID in topological order.
// allNodes: a map of all nodes (nodeID -> node)
// graph: an adjacency list representation of the graph (nodeID -> []neighborIDs)
// startNodeID: the ID of the starting node for traversal
// Returns: a slice of nodes in topological order reachable from startNodeID; or an error if a cycle is detected
func TopologicalSortReachable[T any](allNodes map[string]T, graph map[string][]string, startNodeID string) ([]T, error) {
	if _, ok := graph[startNodeID]; !ok {
		return nil, fmt.Errorf("start node '%s' not found in the graph", startNodeID)
	}
	if _, ok := allNodes[startNodeID]; !ok {
		return nil, fmt.Errorf("start node '%s' not found in the allNodes", startNodeID)
	}

	visitedStatus := make(map[string]NodeState)
	sortedNodes := make([]T, 0)

	for node := range allNodes {
		visitedStatus[node] = UNVISITED
	}

	var dfsVisit func(node string) error
	dfsVisit = func(node string) error {
		visitedStatus[node] = VISITING

		neighbors := graph[node]
		for _, neighbor := range neighbors {
			neighborStatus, statusExists := visitedStatus[neighbor]
			if !statusExists {
				return fmt.Errorf("The key '%s' is in the graph, but not in allNodes", neighbor)
			}

			switch neighborStatus {
			case VISITING:
				return fmt.Errorf("%w: detected edge from %s to %s", CycleError, node, neighbor)
			case UNVISITED:
				if err := dfsVisit(neighbor); err != nil {
					return err
				}
			case VISITED:
				continue
			}
		}

		visitedStatus[node] = VISITED
		sortedNodes = append(sortedNodes, allNodes[node])
		return nil
	}

	if err := dfsVisit(startNodeID); err != nil {
		return nil, err
	}

	for i, j := 0, len(sortedNodes)-1; i < j; i, j = i+1, j-1 {
		sortedNodes[i], sortedNodes[j] = sortedNodes[j], sortedNodes[i]
	}

	return sortedNodes, nil
}

// TopologicalSortAll sorts the nodes in topological order (DFS implementation).
// allNodes: a map of all nodes (nodeID -> node data of type T)
// graph: an adjacency list representation of the graph (nodeID -> []neighborIDs)
// Returns: a slice of nodes (of type T) in topological order; or an error if a cycle is detected
func TopologicalSortAll[T any](allNodes map[string]T, graph map[string][]string) ([]T, error) {
	visited := make(map[string]NodeState)
	result := make([]T, 0, len(allNodes))

	var dfs func(nodeID string) error
	dfs = func(nodeID string) error {
		state := visited[nodeID]
		switch state {
		case VISITING:
			return CycleError
		case VISITED:
			return nil
		case UNVISITED:
			visited[nodeID] = VISITING

			neighbors, exists := graph[nodeID]
			if exists {
				for _, neighborID := range neighbors {
					if _, nodeExists := allNodes[neighborID]; !nodeExists {
						return fmt.Errorf("neighbor node '%s' not found in allNodes", neighborID)
					}

					if err := dfs(neighborID); err != nil {
						return err
					}
				}
			}

			visited[nodeID] = VISITED
			result = append(result, allNodes[nodeID])
			return nil
		}
		return fmt.Errorf("Expected error. This should not happen.")
	}

	for nodeID := range allNodes {
		if visited[nodeID] == UNVISITED {
			if err := dfs(nodeID); err != nil {
				return nil, err
			}
		}
	}

	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	return result, nil
}
