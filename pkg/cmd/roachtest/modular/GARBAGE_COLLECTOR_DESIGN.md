# Garbage Collector Design for Modular Test Framework

## Overview

The Garbage Collector (GC) in the modular test framework provides **automatic cleanup of database objects and cluster state** 
created during test execution. This ensures tests leave the cluster in a clean state, whether they succeed or fail.

## Problem Statement

Roachtests create various database objects during execution:
- Databases, tables, schemas, indexes
- Users and privileges
- Cluster settings modifications
- Zone configuration changes

Without proper cleanup:
1. **Resource leaks**: Objects accumulate across test runs
2. **Test interference**: Leftover state affects subsequent tests
3. **Debugging difficulty**: Hard to distinguish test artifacts from real issues
4. **Manual cleanup burden**: Test authors must remember to clean up everything

## Design Goals

1. **Automatic cleanup**: Objects created via Helper methods are automatically tracked
2. **LIFO ordering**: Cleanup runs in reverse order (last created, first dropped)
3. **Failure resilience**: Cleanup runs even when tests fail
4. **Transparency**: Test authors don't need to write cleanup code
5. **Enforcement**: DDL operations must go through tracked Helper methods

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Test Execution                          │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                    Helper                            │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌───────────┐  │    │
│  │  │ CreateTable  │  │ CreateIndex  │  │ Init      │  │    │
│  │  │ CreateUser   │  │ AddColumn    │  │ Workload      │  │    │
│  │  └──────┬───────┘  └──────┬───────┘  └─────┬─────┘  │    │
│  │         │                 │                │        │    │
│  │         └────────────┬────┴────────────────┘        │    │
│  │                      ▼                              │    │
│  │              RegisterCleanup()                      │    │
│  │                      │                              │    │
│  └──────────────────────┼──────────────────────────────┘    │
│                         ▼                                    │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              GarbageCollector                        │    │
│  │  ┌─────────────────────────────────────────────┐    │    │
│  │  │            Cleanup Stack (LIFO)              │    │    │
│  │  │  ┌────────────────────────────────────────┐ │    │    │
│  │  │  │ DROP INDEX idx_1 ON db.table           │ │    │    │
│  │  │  │ REVOKE SELECT ON TABLE t FROM user     │ │    │    │
│  │  │  │ DROP USER user_123                     │ │    │    │
│  │  │  │ DROP TABLE table_456                   │ │    │    │
│  │  │  │ DROP DATABASE db_789                   │ │    │    │
│  │  │  │ SET CLUSTER SETTING x = 'original'    │ │    │    │
│  │  │  └────────────────────────────────────────┘ │    │    │
│  │  └─────────────────────────────────────────────┘    │    │
│  └─────────────────────────────────────────────────────┘    │
│                         │                                    │
│                         ▼ (on test completion or failure)    │
│                   RunCleanup()                               │
└─────────────────────────────────────────────────────────────┘
```

## Key Components

### 1. GarbageCollector (`garbage_collector.go`)

The central component that manages cleanup registration and execution.

```go
type GarbageCollector struct {
    logger    *logger.Logger
    config    GCConfig
    // cleanupStack stores cleanup statements in LIFO order
    cleanupStack []cleanupEntry
    mu           syncutil.Mutex
}

type cleanupEntry struct {
    description string  // Human-readable description for logging
    statement   string  // SQL statement to execute
}
```

**Key Methods:**
- `RegisterCleanup(description, statement)`: Adds a cleanup entry to the stack
- `RunCleanup(ctx, db)`: Executes all cleanup statements in reverse order
- `Enabled()`: Returns whether GC is enabled

### 2. Helper Methods with Auto-Registration

Each DDL Helper method automatically registers its corresponding cleanup:

| Helper Method                        | Cleanup Registered |
|--------------------------------------|-------------------|
| `CreateTable(name, schema)`          | `DROP TABLE IF EXISTS name CASCADE` |
| `CreateDatabase(name)`               | `DROP DATABASE IF EXISTS name CASCADE` |
| `CreateIndex(name, db, table, cols)` | `DROP INDEX IF EXISTS db.table@name CASCADE` |
| `CreateUser(name)`                   | `DROP USER IF EXISTS name` |
| `CreateSchema(name)`                 | `DROP SCHEMA IF EXISTS name CASCADE` |
| `AddColumn(db, table, col, type)`    | `ALTER TABLE db.table DROP COLUMN col CASCADE` |
| `Grant(priv, type, obj, grantee)`    | `REVOKE priv ON type obj FROM grantee` |
| `SetClusterSetting(name, value)`     | `SET CLUSTER SETTING name = 'original_value'` |
| `InitWorkload(workload)`             | `DROP DATABASE IF EXISTS workload CASCADE` |
|`RunWorkload(workload)`|

### 3. DDL Validation in Exec()

The `Exec()` method validates that DDL statements are not passed directly:

```go
func (h *Helper) Exec(query string, args ...interface{}) error {
    validateNotDDL(query)  // Panics if DDL detected
    return h.defaultService.Exec(h.rng, query, args...)
}

var ddlPrefixes = []string{
    "CREATE TABLE", "CREATE INDEX", "CREATE DATABASE", "CREATE SCHEMA",
    "CREATE USER", "ALTER TABLE", "ALTER INDEX", "ALTER DATABASE",
    "SET CLUSTER SETTING", "GRANT", "REVOKE", "TRUNCATE",
    // ... more
}
```

This ensures DDL operations go through tracked Helper methods.

## Cleanup Execution Flow

```
Test Completes (success or failure)
         │
         ▼
    RunTestPlan()
         │
         ▼
    defer helper.RunCleanup()
         │
         ▼
    GarbageCollector.RunCleanup()
         │
         ├── Pop cleanup entry from stack
         ├── Log: "GC: Executing cleanup: <description>"
         ├── Execute SQL statement
         ├── Log success or warning on error
         └── Continue until stack is empty
```

## For final implementation

### 1. Static Analysis Linter (High Priority)

Currently, DDL validation happens at runtime via `validateNotDDL()`. This catches violations but only when the code path is executed. 
We need a **static analysis linter** to catch DDL usage in `Exec()` at build/CI time.

**Proposed Implementation:**

```go
// Linter rule: modular-no-ddl-in-exec
//
// Checks for DDL statements passed to h.Exec() or h.ExecWithGateway()
// and suggests the appropriate Helper method.
//
// Example violation:
//   h.Exec("CREATE TABLE foo (id INT)")
//
// Suggested fix:
//   h.CreateTable("foo", "id INT")
```

**Integration Points:**
- Add to `./dev lint` checks
- Add to CI pipeline
- Provide auto-fix suggestions where possible


## Testing the GC

The `modular/gc-schemachange` test exercises the GC system:

```bash
./dev test pkg/cmd/roachtest/tests -f=TestModularGCSchemaChange -v
```

This test:
1. Creates databases, tables, indexes, users
2. Grants privileges
3. Modifies cluster settings
4. Runs a workload
5. Verifies all objects are cleaned up after test completion


### Snapshots
1. Taking the database snapshot before running the test.
2. Execute the test with garbage collection.
3. Take snapshot and compare to old one to find deltas.
