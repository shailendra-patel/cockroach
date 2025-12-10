// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package modular

import (
	"context"
	gosql "database/sql"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

// GCConfig configures garbage collection behavior.
type GCConfig struct {
	// Timeout is the maximum time to wait for all cleanup statements.
	// Default: 5 minutes.
	Timeout time.Duration
	// ContinueOnError indicates whether to continue cleanup if individual statements fail.
	// Default: true.
	ContinueOnError bool
	// Execute indicates whether to actually execute cleanup statements.
	// When false, operates in dry-run mode (logs but doesn't execute).
	// Default: true.
	Execute bool
	// Enabled indicates whether GC functionality is enabled.
	// Default: true.
	Enabled bool
}

// DefaultGCConfig returns the default GC configuration.
func DefaultGCConfig() GCConfig {
	return GCConfig{
		Timeout:         5 * time.Minute,
		ContinueOnError: true,
		Execute:         true,
		Enabled:         true,
	}
}

// GarbageCollector manages cleanup for a single test plan execution.
// It owns a CleanupStack for global objects (cluster settings, zone configs,
// external databases) and a PlanSchema for schema-scoped object isolation.
//
// Schema-scoped objects (tables, indexes) are NOT individually registered;
// they are cleaned up via DROP SCHEMA CASCADE at the end.
type GarbageCollector struct {
	stack    *CleanupStack
	schema   *PlanSchema
	connFunc func() *gosql.DB
	logger   *logger.Logger
	config   GCConfig
}

// NewGarbageCollector creates a new GarbageCollector for a test plan.
// The connFunc should return a new database connection each time it's called.
func NewGarbageCollector(
	seed int64,
	connFunc func() *gosql.DB,
	l *logger.Logger,
	config GCConfig,
) *GarbageCollector {
	return &GarbageCollector{
		stack:    NewCleanupStack(l),
		schema:   NewPlanSchema(seed),
		connFunc: connFunc,
		logger:   l,
		config:   config,
	}
}

// Stack returns the cleanup stack for registering global/external cleanup entries.
// Use this only for:
// - Cluster settings (global)
// - Zone configurations (global)
// - External databases (created outside plan schema)
// - Failure injection recovery
//
// Do NOT use for schema-scoped objects (tables, indexes) - CASCADE handles them.
func (gc *GarbageCollector) Stack() *CleanupStack {
	return gc.stack
}

// Schema returns the plan's schema.
func (gc *GarbageCollector) Schema() *PlanSchema {
	return gc.schema
}

// Enabled returns whether GC is enabled.
func (gc *GarbageCollector) Enabled() bool {
	return gc.config.Enabled
}

// Initialize creates the plan schema.
// This should be called at the start of test plan execution.
// The schema drop is NOT registered here - it's handled explicitly in ExecuteCleanup.
func (gc *GarbageCollector) Initialize(ctx context.Context) error {
	if !gc.config.Enabled {
		gc.logger.Printf("[GC] GC is disabled, skipping initialization")
		return nil
	}

	gc.logger.Printf("[GC] Creating plan schema: %s", gc.schema.Name)

	db := gc.connFunc()
	defer db.Close()

	// Create the schema
	_, err := db.ExecContext(ctx, gc.schema.CreateSQL())
	if err != nil {
		return fmt.Errorf("failed to create plan schema %s: %w", gc.schema.Name, err)
	}

	gc.logger.Printf("[GC] Plan schema %s created successfully", gc.schema.Name)
	return nil
}

// SetSearchPath sets the database connection's search_path to the plan schema.
// This ensures that unqualified table names are created in the plan schema.
func (gc *GarbageCollector) SetSearchPath(ctx context.Context, db *gosql.DB) error {
	if !gc.config.Enabled {
		return nil
	}

	_, err := db.ExecContext(ctx, gc.schema.SetSearchPathSQL())
	if err != nil {
		return fmt.Errorf("failed to set search_path to %s: %w", gc.schema.Name, err)
	}
	return nil
}

// ExecuteCleanup runs all cleanup statements in LIFO order, then drops the plan schema.
// This should be called when the test plan completes (success or failure).
//
// Cleanup order:
// 1. Pop and execute all entries in CleanupStack (LIFO) - cluster settings, zone configs, etc.
// 2. Drop the plan schema with CASCADE - cleans up all tables, indexes, etc.
func (gc *GarbageCollector) ExecuteCleanup(ctx context.Context) error {
	if !gc.config.Enabled {
		gc.logger.Printf("[GC] GC is disabled, skipping cleanup")
		return nil
	}

	// Create a timeout context for cleanup
	cleanupCtx, cancel := context.WithTimeout(ctx, gc.config.Timeout)
	defer cancel()

	var errors []error

	// Phase 1: Execute all entries in the cleanup stack (global objects)
	stackSize := gc.stack.Size()
	gc.logger.Printf("[GC] Starting cleanup, %d entries in stack", stackSize)

	executed := 0
	for {
		entry := gc.stack.Pop()
		if entry == nil {
			break // Stack is empty
		}

		executed++
		gc.logger.Printf("[GC] Cleanup [%d/%d]: %s", executed, stackSize, entry.Description)

		if !gc.config.Execute {
			gc.logger.Printf("[GC]   (dry run, skipping execution)")
			continue
		}

		err := gc.executeStatement(cleanupCtx, entry.Statement)
		if err != nil {
			gc.logger.Printf("[GC]   ERROR: %v", err)
			errors = append(errors, fmt.Errorf("%s: %w", entry.Description, err))

			// Check if we should stop on this error
			if !entry.ContinueOnError && !gc.config.ContinueOnError {
				return fmt.Errorf("cleanup failed at entry %d: %w", executed, errors[0])
			}
		} else {
			gc.logger.Printf("[GC]   OK")
		}
	}

	// Phase 2: Drop the plan schema (CASCADE cleans up all schema-scoped objects)
	gc.logger.Printf("[GC] Dropping plan schema: %s", gc.schema.Name)

	if gc.config.Execute {
		err := gc.executeStatement(cleanupCtx, gc.schema.DropSQL())
		if err != nil {
			gc.logger.Printf("[GC]   ERROR dropping schema: %v", err)
			errors = append(errors, fmt.Errorf("drop schema %s: %w", gc.schema.Name, err))
		} else {
			gc.logger.Printf("[GC]   OK - schema dropped")
		}
	} else {
		gc.logger.Printf("[GC]   (dry run, skipping schema drop)")
	}

	gc.logger.Printf("[GC] Cleanup complete: %d stack entries executed, %d errors", executed, len(errors))

	if len(errors) > 0 {
		return fmt.Errorf("cleanup completed with %d errors", len(errors))
	}
	return nil
}

// executeStatement executes a single SQL statement for cleanup.
func (gc *GarbageCollector) executeStatement(ctx context.Context, stmt string) error {
	db := gc.connFunc()
	defer db.Close()

	_, err := db.ExecContext(ctx, stmt)
	return err
}

// RegisterCleanup is a convenience method to register a cleanup statement.
// Use this for objects created outside of the plan schema (external databases, etc.).
func (gc *GarbageCollector) RegisterCleanup(description, statement string) {
	if gc.config.Enabled {
		gc.stack.PushSQL(description, statement)
	}
}