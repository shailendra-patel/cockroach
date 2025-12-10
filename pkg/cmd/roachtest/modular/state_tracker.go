package modular

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/failureinjection/failures"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

// ClusterStateTracker tracks cluster state changes made during test execution.
// With the GC system, this is simplified to only track:
// - Failure injections (require explicit recovery, not SQL cleanup)
// - Name generation for unique object names
//
// SQL object cleanup (tables, databases, schemas, cluster settings, zone configs)
// is now handled by the GarbageCollector via LIFO cleanup stack and plan schema isolation.
type ClusterStateTracker struct {
	mu sync.RWMutex

	// failuresInjected tracks failure injection operations that need recovery
	failuresInjected map[string]*failures.Failer

	// debugLogger logs debug information for tracking operations
	debugLogger *logger.Logger
}

// NewClusterStateTracker creates a new cluster state tracker with initialized maps.
func NewClusterStateTracker(debugLogger *logger.Logger) *ClusterStateTracker {
	return &ClusterStateTracker{
		failuresInjected: make(map[string]*failures.Failer),
		debugLogger:      debugLogger,
	}
}

// NewTableName generates a unique table name with the given prefix.
// The table will be created in the plan schema by the Helper.
func (c *ClusterStateTracker) NewTableName(namePrefix string) string {
	return fmt.Sprintf("%s_%d", namePrefix, time.Now().UnixNano())
}

// NewUsername generates a unique username with the given prefix.
func (c *ClusterStateTracker) NewUsername(namePrefix string) string {
	return fmt.Sprintf("%s_%d", namePrefix, time.Now().UnixNano())
}

// NewDatabaseName generates a unique database name with the given prefix.
func (c *ClusterStateTracker) NewDatabaseName(namePrefix string) string {
	return fmt.Sprintf("%s_%d", namePrefix, time.Now().UnixNano())
}

// NewSchemaName generates a unique schema name with the given prefix.
func (c *ClusterStateTracker) NewSchemaName(namePrefix string) string {
	return fmt.Sprintf("%s_%d", namePrefix, time.Now().UnixNano())
}

// NewIndexName generates a unique index name with the given prefix.
func (c *ClusterStateTracker) NewIndexName(namePrefix string) string {
	return fmt.Sprintf("%s_%d", namePrefix, time.Now().UnixNano())
}

// TrackFailureInjected records that a failure was injected.
// Failure injections require explicit recovery via Failer.Recover(),
// which is different from SQL cleanup handled by GC.
func (c *ClusterStateTracker) TrackFailureInjected(failureID string, failer *failures.Failer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failuresInjected[failureID] = failer
	c.debugLogger.Printf("Tracked failure injection: %s", failureID)
}

// GetTrackedFailures returns a copy of all tracked failure injections.
func (c *ClusterStateTracker) GetTrackedFailures() map[string]*failures.Failer {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]*failures.Failer)
	for id, failer := range c.failuresInjected {
		result[id] = failer
	}
	return result
}

// LogClusterStateDebug logs the current cluster state for debugging purposes.
func (c *ClusterStateTracker) LogClusterStateDebug(context string) {
	c.debugLogger.Printf("Cluster State %s:", context)
	stateOutput := c.PrintTrackedState()
	c.debugLogger.Printf("%s", stateOutput)
}

// PrintTrackedState returns a formatted string representation of tracked state.
// With GC, this is simplified to only show failure injections.
func (c *ClusterStateTracker) PrintTrackedState() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var output []string
	output = append(output, "=== TRACKED CLUSTER STATE ===")

	if len(c.failuresInjected) == 0 {
		output = append(output, "No failure injections tracked")
		output = append(output, "(SQL object cleanup handled by GC)")
		output = append(output, "=============================")
		return strings.Join(output, "\n")
	}

	output = append(output, fmt.Sprintf("Failures injected (%d):", len(c.failuresInjected)))
	for failureID, failer := range c.failuresInjected {
		description := failer.Description()
		output = append(output, fmt.Sprintf("  - %s: %s", failureID, description))
	}
	output = append(output, "")
	output = append(output, "(SQL object cleanup handled by GC)")
	output = append(output, "=============================")

	return strings.Join(output, "\n")
}