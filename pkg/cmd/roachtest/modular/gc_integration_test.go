// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package modular

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/stretchr/testify/require"
)

// TestGCIntegration tests the garbage collection system with actual database operations.
// This is a unit test that verifies the GC components work correctly together.
func TestGCIntegration(t *testing.T) {
	t.Run("cleanup stack LIFO order", func(t *testing.T) {
		l, err := logger.RootLogger("", false /* tee */)
		require.NoError(t, err)

		stack := NewCleanupStack(l)

		// Push entries in order: A, B, C
		stack.PushSQL("Drop A", "DROP TABLE a")
		stack.PushSQL("Drop B", "DROP TABLE b")
		stack.PushSQL("Drop C", "DROP TABLE c")

		require.Equal(t, 3, stack.Size())

		// Pop should return in LIFO order: C, B, A
		entry := stack.Pop()
		require.NotNil(t, entry)
		require.Equal(t, "Drop C", entry.Description)

		entry = stack.Pop()
		require.NotNil(t, entry)
		require.Equal(t, "Drop B", entry.Description)

		entry = stack.Pop()
		require.NotNil(t, entry)
		require.Equal(t, "Drop A", entry.Description)

		// Stack should be empty now
		require.Nil(t, stack.Pop())
		require.True(t, stack.IsEmpty())
	})

	t.Run("plan schema naming", func(t *testing.T) {
		schema := NewPlanSchema(12345)
		require.Equal(t, "test_plan_12345", schema.Name)
		require.Equal(t, "defaultdb", schema.Database)
		require.Equal(t, "defaultdb.test_plan_12345", schema.FullyQualifiedName())

		// Test negative seed handling - should prefix with 'n' to create valid SQL identifier
		negativeSchema := NewPlanSchema(-12345)
		require.Equal(t, "test_plan_n12345", negativeSchema.Name)
		require.Equal(t, "defaultdb.test_plan_n12345", negativeSchema.FullyQualifiedName())
	})

	t.Run("plan schema SQL generation", func(t *testing.T) {
		schema := NewPlanSchema(99999)

		createSQL := schema.CreateSQL()
		require.Contains(t, createSQL, "CREATE SCHEMA")
		require.Contains(t, createSQL, "test_plan_99999")

		dropSQL := schema.DropSQL()
		require.Contains(t, dropSQL, "DROP SCHEMA")
		require.Contains(t, dropSQL, "CASCADE")
		require.Contains(t, dropSQL, "test_plan_99999")

		qualifiedTable := schema.QualifyTable("my_table")
		require.Equal(t, "defaultdb.test_plan_99999.my_table", qualifiedTable)

		searchPathSQL := schema.SetSearchPathSQL()
		require.Contains(t, searchPathSQL, "SET search_path")
		require.Contains(t, searchPathSQL, "test_plan_99999")
	})

	t.Run("GC config defaults", func(t *testing.T) {
		config := DefaultGCConfig()
		require.True(t, config.Enabled)
		require.True(t, config.Execute)
		require.True(t, config.ContinueOnError)
		require.NotZero(t, config.Timeout)
	})

	t.Run("GC disabled skips operations", func(t *testing.T) {
		l, err := logger.RootLogger("", false /* tee */)
		require.NoError(t, err)

		config := GCConfig{Enabled: false}
		gc := NewGarbageCollector(12345, nil, l, config)

		require.False(t, gc.Enabled())

		// Initialize should be a no-op when disabled
		err = gc.Initialize(context.Background())
		require.NoError(t, err)

		// Cleanup should be a no-op when disabled
		err = gc.ExecuteCleanup(context.Background())
		require.NoError(t, err)
	})
}

// TestCleanupStackConcurrency tests thread-safety of the cleanup stack.
func TestCleanupStackConcurrency(t *testing.T) {
	l, err := logger.RootLogger("", false /* tee */)
	require.NoError(t, err)

	stack := NewCleanupStack(l)

	// Launch multiple goroutines to push entries concurrently
	done := make(chan struct{})
	numGoroutines := 10
	entriesPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			for j := 0; j < entriesPerGoroutine; j++ {
				stack.PushSQL(
					fmt.Sprintf("Entry %d-%d", goroutineID, j),
					fmt.Sprintf("DROP TABLE t_%d_%d", goroutineID, j),
				)
			}
			done <- struct{}{}
		}(i)
	}

	// Wait for all goroutines to finish
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify all entries were added
	require.Equal(t, numGoroutines*entriesPerGoroutine, stack.Size())

	// Pop all entries (should not panic)
	count := 0
	for stack.Pop() != nil {
		count++
	}
	require.Equal(t, numGoroutines*entriesPerGoroutine, count)
}

// TestStateTrackerNameGeneration tests unique name generation.
func TestStateTrackerNameGeneration(t *testing.T) {
	l, err := logger.RootLogger("", false /* tee */)
	require.NoError(t, err)

	tracker := NewClusterStateTracker(l)

	// Generate names and verify they're unique
	names := make(map[string]struct{})

	for i := 0; i < 100; i++ {
		tableName := tracker.NewTableName("test")
		require.NotContains(t, names, tableName, "duplicate table name generated")
		names[tableName] = struct{}{}

		userName := tracker.NewUsername("user")
		require.NotContains(t, names, userName, "duplicate user name generated")
		names[userName] = struct{}{}

		dbName := tracker.NewDatabaseName("db")
		require.NotContains(t, names, dbName, "duplicate database name generated")
		names[dbName] = struct{}{}
	}
}