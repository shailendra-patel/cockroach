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
		Timeout: 5 * time.Minute,
		Execute: true,
		Enabled: true,
	}
}

// GarbageCollector manages cleanup for a single test plan execution.
// It owns a CleanupStack for global objects (cluster settings, zone configs,
// external databases)
type GarbageCollector struct {
	stack  *CleanupStack
	logger *logger.Logger
	config GCConfig
}

// NewGarbageCollector creates a new GarbageCollector for a test plan.
// The connFunc should return a new database connection each time it's called.
func NewGarbageCollector(
	l *logger.Logger,
	config GCConfig,
) *GarbageCollector {
	return &GarbageCollector{
		stack:  NewCleanupStack(l),
		logger: l,
		config: config,
	}
}

// Enabled returns whether GC is enabled.
func (gc *GarbageCollector) Enabled() bool {
	return gc.config.Enabled
}

// RegisterCleanup is a convenience method to register a cleanup statement.
// Use this for objects created outside of the plan schema (external databases, etc.).
func (gc *GarbageCollector) RegisterCleanup(description, statement string) {
	if gc.config.Enabled {
		gc.stack.PushSQL(description, statement)
	}
}

// RunCleanup runs all cleanup statements in LIFO order, then drops the plan schema.
// This should be called when the test plan completes (success or failure).
//
// Cleanup order:
// 1. Pop and execute all entries in CleanupStack (LIFO) - cluster settings, zone configs, etc.
// 2. Drop the plan schema with CASCADE - cleans up all tables, indexes, etc.
func (gc *GarbageCollector) RunCleanup(ctx context.Context, db *gosql.DB) error {
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

		err := gc.executeStatement(cleanupCtx, db, entry.Statement)
		if err != nil {
			gc.logger.Printf("[GC]   ERROR: %v", err)
			errors = append(errors, fmt.Errorf("%s: %w", entry.Description, err))
		}
	}

	gc.logger.Printf("[GC] Cleanup complete: %d stack entries executed, %d errors", executed, len(errors))

	if len(errors) > 0 {
		return fmt.Errorf("cleanup completed with %d errors", len(errors))
	}
	return nil
}

// executeStatement executes a single SQL statement for cleanup.
func (gc *GarbageCollector) executeStatement(ctx context.Context, db *gosql.DB, stmt string) error {
	_, err := db.ExecContext(ctx, stmt)
	return err
}
