# Plan-Scoped GC Design: LIFO Cleanup Stack

## Design Overview

Each test plan execution operates in its own database schema and maintains a LIFO (Last-In-First-Out) stack of cleanup SQL statements. Helper methods automatically register cleanup statements when creating objects. The Runner owns the GC and executes cleanup when the plan completes or fails.

```
┌─────────────────────────────────────────────────────────────┐
│                      Test Plan Execution                     │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                    Runner                            │    │
│  │  ┌───────────────────────────────────────────────┐  │    │
│  │  │              Cleanup Stack (LIFO)              │  │    │
│  │  │  ┌─────────────────────────────────────────┐  │  │    │
│  │  │  │ DROP INDEX idx_3                        │  │  │    │
│  │  │  │ DROP TABLE plan_abc123.users            │  │  │    │
│  │  │  │ DROP TABLE plan_abc123.orders           │  │  │    │
│  │  │  │ RESET CLUSTER SETTING kv.snapshot...    │  │  │    │
│  │  │  │ DROP SCHEMA plan_abc123 CASCADE         │  │  │    │
│  │  │  └─────────────────────────────────────────┘  │  │    │
│  │  └───────────────────────────────────────────────┘  │    │
│  └─────────────────────────────────────────────────────┘    │
│                              │                               │
│                              ▼                               │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                     Helper                           │    │
│  │  CreateTable() ──► Push("DROP TABLE ...")           │    │
│  │  CreateIndex() ──► Push("DROP INDEX ...")           │    │
│  │  SetClusterSetting() ──► Push("RESET ...")          │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼ (on plan complete/fail)
┌─────────────────────────────────────────────────────────────┐
│                    Cleanup Execution                         │
│  Pop and execute each statement in LIFO order:              │
│  1. DROP INDEX idx_3                                        │
│  2. DROP TABLE plan_abc123.users                            │
│  3. DROP TABLE plan_abc123.orders                           │
│  4. RESET CLUSTER SETTING kv.snapshot...                    │
│  5. DROP SCHEMA plan_abc123 CASCADE (final safety net)      │
└─────────────────────────────────────────────────────────────┘
```

---

## Core Components

### 1. CleanupStack

Thread-safe LIFO stack of cleanup statements.

```go
// cleanup_stack.go

package modular

import (
    "context"
    "sync"

    "github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

// CleanupEntry represents a single cleanup action.
type CleanupEntry struct {
    // SQL statement to execute for cleanup
    Statement string
    // Human-readable description for logging
    Description string
    // If true, continue cleanup even if this statement fails
    ContinueOnError bool
}

// CleanupStack is a thread-safe LIFO stack of cleanup entries.
type CleanupStack struct {
    mu      sync.Mutex
    entries []CleanupEntry
    logger  *logger.Logger
}

// NewCleanupStack creates a new cleanup stack.
func NewCleanupStack(l *logger.Logger) *CleanupStack {
    return &CleanupStack{
        entries: make([]CleanupEntry, 0),
        logger:  l,
    }
}

// Push adds a cleanup entry to the top of the stack.
func (cs *CleanupStack) Push(entry CleanupEntry) {
    cs.mu.Lock()
    defer cs.mu.Unlock()
    cs.entries = append(cs.entries, entry)
    cs.logger.Printf("Registered cleanup: %s", entry.Description)
}

// PushSQL is a convenience method for pushing a SQL cleanup statement.
func (cs *CleanupStack) PushSQL(description, statement string) {
    cs.Push(CleanupEntry{
        Statement:       statement,
        Description:     description,
        ContinueOnError: true, // Default: don't fail cleanup on individual errors
    })
}

// Pop removes and returns the top entry. Returns nil if empty.
func (cs *CleanupStack) Pop() *CleanupEntry {
    cs.mu.Lock()
    defer cs.mu.Unlock()
    if len(cs.entries) == 0 {
        return nil
    }
    entry := cs.entries[len(cs.entries)-1]
    cs.entries = cs.entries[:len(cs.entries)-1]
    return &entry
}

// Size returns the number of entries in the stack.
func (cs *CleanupStack) Size() int {
    cs.mu.Lock()
    defer cs.mu.Unlock()
    return len(cs.entries)
}

// Clear removes all entries without executing them.
func (cs *CleanupStack) Clear() {
    cs.mu.Lock()
    defer cs.mu.Unlock()
    cs.entries = cs.entries[:0]
}

// Entries returns a copy of all entries (for inspection/debugging).
func (cs *CleanupStack) Entries() []CleanupEntry {
    cs.mu.Lock()
    defer cs.mu.Unlock()
    result := make([]CleanupEntry, len(cs.entries))
    copy(result, cs.entries)
    return result
}
```

### 2. PlanSchema

Each plan gets its own schema for isolation.

```go
// plan_schema.go

package modular

import (
    "context"
    "fmt"

    "github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

// PlanSchema manages the schema for a single plan execution.
type PlanSchema struct {
    // Unique schema name for this plan (e.g., "plan_abc123")
    Name string
    // Database where schema is created (typically "defaultdb")
    Database string
}

// NewPlanSchema generates a unique schema for this plan execution.
func NewPlanSchema(seed int64) *PlanSchema {
    // Use seed to generate reproducible schema name
    schemaName := fmt.Sprintf("plan_%d", seed)
    return &PlanSchema{
        Name:     schemaName,
        Database: "defaultdb",
    }
}

// FullyQualifiedName returns database.schema format.
func (ps *PlanSchema) FullyQualifiedName() string {
    return fmt.Sprintf("%s.%s", ps.Database, ps.Name)
}

// CreateSQL returns the SQL to create this schema.
func (ps *PlanSchema) CreateSQL() string {
    return fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s", ps.Database, ps.Name)
}

// DropSQL returns the SQL to drop this schema and all contained objects.
func (ps *PlanSchema) DropSQL() string {
    return fmt.Sprintf("DROP SCHEMA IF EXISTS %s.%s CASCADE", ps.Database, ps.Name)
}

// QualifyTable returns a fully qualified table name within this schema.
func (ps *PlanSchema) QualifyTable(tableName string) string {
    return fmt.Sprintf("%s.%s.%s", ps.Database, ps.Name, tableName)
}
```

### 3. GarbageCollector

Owned by Runner, coordinates cleanup execution.

```go
// garbage_collector.go

package modular

import (
    "context"
    "fmt"
    "time"

    gosql "database/sql"
    "github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

// GCConfig configures garbage collection behavior.
type GCConfig struct {
    // Maximum time to wait for all cleanup statements
    Timeout time.Duration
    // Whether to continue cleanup if individual statements fail
    ContinueOnError bool
    // Whether to execute cleanup (false = dry run for testing)
    Execute bool
}

// DefaultGCConfig returns sensible defaults.
func DefaultGCConfig() GCConfig {
    return GCConfig{
        Timeout:         5 * time.Minute,
        ContinueOnError: true,
        Execute:         true,
    }
}

// GarbageCollector manages cleanup for a single plan execution.
type GarbageCollector struct {
    stack      *CleanupStack
    schema     *PlanSchema
    connFunc   func() *gosql.DB
    logger     *logger.Logger
    config     GCConfig
}

// NewGarbageCollector creates a GC for a plan execution.
func NewGarbageCollector(
    schema *PlanSchema,
    connFunc func() *gosql.DB,
    l *logger.Logger,
    config GCConfig,
) *GarbageCollector {
    return &GarbageCollector{
        stack:    NewCleanupStack(l),
        schema:   schema,
        connFunc: connFunc,
        logger:   l,
        config:   config,
    }
}

// Stack returns the cleanup stack for registering entries.
func (gc *GarbageCollector) Stack() *CleanupStack {
    return gc.stack
}

// Schema returns the plan's schema.
func (gc *GarbageCollector) Schema() *PlanSchema {
    return gc.schema
}

// Initialize creates the plan schema.
func (gc *GarbageCollector) Initialize(ctx context.Context) error {
    gc.logger.Printf("Creating plan schema: %s", gc.schema.Name)

    db := gc.connFunc()
    defer db.Close()

    _, err := db.ExecContext(ctx, gc.schema.CreateSQL())
    if err != nil {
        return fmt.Errorf("failed to create plan schema: %w", err)
    }

    // Register schema drop as the FIRST cleanup entry (will be executed LAST)
    gc.stack.PushSQL(
        fmt.Sprintf("Drop plan schema %s", gc.schema.Name),
        gc.schema.DropSQL(),
    )

    return nil
}

// ExecuteCleanup runs all cleanup statements in LIFO order.
func (gc *GarbageCollector) ExecuteCleanup(ctx context.Context) error {
    gc.logger.Printf("Starting cleanup, %d entries in stack", gc.stack.Size())

    // Create a timeout context for cleanup
    cleanupCtx, cancel := context.WithTimeout(ctx, gc.config.Timeout)
    defer cancel()

    var errors []error
    executed := 0

    for {
        entry := gc.stack.Pop()
        if entry == nil {
            break // Stack empty
        }

        gc.logger.Printf("Cleanup [%d]: %s", executed+1, entry.Description)

        if !gc.config.Execute {
            gc.logger.Printf("  (dry run, skipping execution)")
            executed++
            continue
        }

        err := gc.executeStatement(cleanupCtx, entry.Statement)
        if err != nil {
            gc.logger.Printf("  ERROR: %v", err)
            errors = append(errors, fmt.Errorf("%s: %w", entry.Description, err))

            if !entry.ContinueOnError && !gc.config.ContinueOnError {
                return fmt.Errorf("cleanup failed: %w", errors[0])
            }
        } else {
            gc.logger.Printf("  OK")
        }
        executed++
    }

    gc.logger.Printf("Cleanup complete: %d executed, %d errors", executed, len(errors))

    if len(errors) > 0 {
        return fmt.Errorf("cleanup completed with %d errors", len(errors))
    }
    return nil
}

func (gc *GarbageCollector) executeStatement(ctx context.Context, stmt string) error {
    db := gc.connFunc()
    defer db.Close()

    _, err := db.ExecContext(ctx, stmt)
    return err
}
```

### 4. Updated Helper

Helper methods register cleanup statements automatically.

```go
// helper.go (updated methods)

// CreateTable creates a table in the plan's schema and registers cleanup.
func (h *Helper) CreateTable(name, schema string) (string, error) {
    // Qualify table name with plan schema
    qualifiedName := h.gc.Schema().QualifyTable(name)

    createSQL := fmt.Sprintf("CREATE TABLE %s (%s)", qualifiedName, schema)
    err := h.Exec(createSQL)
    if err != nil {
        return "", fmt.Errorf("failed to create table %s: %w", qualifiedName, err)
    }

    // Register cleanup (pushed after create, so popped before schema drop)
    h.gc.Stack().PushSQL(
        fmt.Sprintf("Drop table %s", qualifiedName),
        fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", qualifiedName),
    )

    return qualifiedName, nil
}

// CreateIndex creates an index and registers cleanup.
func (h *Helper) CreateIndex(name, table string, columns []string) (string, error) {
    columnsStr := strings.Join(columns, ", ")
    createSQL := fmt.Sprintf("CREATE INDEX %s ON %s (%s)", name, table, columnsStr)

    err := h.Exec(createSQL)
    if err != nil {
        return "", fmt.Errorf("failed to create index %s: %w", name, err)
    }

    // Register cleanup
    h.gc.Stack().PushSQL(
        fmt.Sprintf("Drop index %s", name),
        fmt.Sprintf("DROP INDEX IF EXISTS %s CASCADE", name),
    )

    return name, nil
}

// SetClusterSetting sets a cluster setting and registers cleanup to reset it.
func (h *Helper) SetClusterSetting(name, value string) error {
    // First, capture the current value for restoration
    var currentValue string
    row := h.QueryRow(fmt.Sprintf("SHOW CLUSTER SETTING %s", name))
    if err := row.Scan(&currentValue); err != nil {
        return fmt.Errorf("failed to read current value of %s: %w", name, err)
    }

    // Set the new value
    setSQL := fmt.Sprintf("SET CLUSTER SETTING %s = $1", name)
    if err := h.Exec(setSQL, value); err != nil {
        return fmt.Errorf("failed to set cluster setting %s: %w", name, err)
    }

    // Register cleanup to restore original value
    restoreSQL := fmt.Sprintf("SET CLUSTER SETTING %s = '%s'", name, currentValue)
    h.gc.Stack().PushSQL(
        fmt.Sprintf("Restore cluster setting %s to '%s'", name, currentValue),
        restoreSQL,
    )

    return nil
}

// ResetClusterSetting resets a setting to default and registers cleanup.
func (h *Helper) ResetClusterSetting(name string) error {
    // Capture current value first
    var currentValue string
    row := h.QueryRow(fmt.Sprintf("SHOW CLUSTER SETTING %s", name))
    if err := row.Scan(&currentValue); err != nil {
        return fmt.Errorf("failed to read current value of %s: %w", name, err)
    }

    // Reset to default
    resetSQL := fmt.Sprintf("RESET CLUSTER SETTING %s", name)
    if err := h.Exec(resetSQL); err != nil {
        return fmt.Errorf("failed to reset cluster setting %s: %w", name, err)
    }

    // Register cleanup to restore to pre-reset value
    restoreSQL := fmt.Sprintf("SET CLUSTER SETTING %s = '%s'", name, currentValue)
    h.gc.Stack().PushSQL(
        fmt.Sprintf("Restore cluster setting %s to '%s'", name, currentValue),
        restoreSQL,
    )

    return nil
}
```

### 5. Updated Runner

Runner owns GC and triggers cleanup.

```go
// runner.go (updated)

type Runner struct {
    testPlan *TestPlan
    helper   *Helper
    gc       *GarbageCollector
}

func NewRunner(testPlan *TestPlan) *Runner {
    // Create plan-scoped schema
    schema := NewPlanSchema(testPlan.seed)

    // Create GC
    gc := NewGarbageCollector(
        schema,
        func() *gosql.DB { return testPlan.cluster.Conn(testPlan.ctx, testPlan.logger, 1) },
        testPlan.logger,
        DefaultGCConfig(),
    )

    return &Runner{
        testPlan: testPlan,
        helper:   &Helper{rng: testPlan.rng, gc: gc},
        gc:       gc,
    }
}

func (r *Runner) Run(ctx context.Context, t test.Test) error {
    l := t.L()

    // Initialize plan schema
    if err := r.gc.Initialize(ctx); err != nil {
        return fmt.Errorf("failed to initialize GC: %w", err)
    }

    // Always run cleanup at the end (success or failure)
    defer func() {
        l.Printf("Executing cleanup...")
        if err := r.gc.ExecuteCleanup(ctx); err != nil {
            l.Printf("Cleanup completed with errors: %v", err)
        }
    }()

    // Initialize helper
    r.initializeHelper(ctx, t)

    // Execute test plan
    return r.executeSteps(ctx, l)
}
```

---

## Execution Example

### Test Code
```go
func myTest(ctx context.Context, t test.Test, c cluster.Cluster) {
    mt := modular.NewTest(ctx, t.L(), c, c.CRDBNodes())

    stage := mt.NewStage("test")
    mt.InStage(stage, "create schema", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
        // Creates: plan_12345.users
        // Registers: DROP TABLE plan_12345.users CASCADE
        _, err := h.CreateTable("users", "id INT PRIMARY KEY, name TEXT")
        return err
    }).Then("add index", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
        // Creates: users_name_idx
        // Registers: DROP INDEX users_name_idx CASCADE
        _, err := h.CreateIndex("users_name_idx", h.gc.Schema().QualifyTable("users"), []string{"name"})
        return err
    }).Then("modify settings", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
        // Sets: kv.snapshot_rebalance.max_rate = '2 GiB'
        // Registers: SET CLUSTER SETTING kv.snapshot_rebalance.max_rate = '<original>'
        return h.SetClusterSetting("kv.snapshot_rebalance.max_rate", "2 GiB")
    })

    // ... run test
}
```

### Cleanup Stack State (After All Steps)
```
Bottom ─────────────────────────────────────────────────── Top
│ DROP SCHEMA plan_12345 CASCADE                            │
│ DROP TABLE plan_12345.users CASCADE                       │
│ DROP INDEX users_name_idx CASCADE                         │
│ SET CLUSTER SETTING kv.snapshot_rebalance.max_rate = '...'│
└───────────────────────────────────────────────────────────┘
```

### Cleanup Execution Order (LIFO)
```
1. SET CLUSTER SETTING kv.snapshot_rebalance.max_rate = '<original>'
2. DROP INDEX users_name_idx CASCADE
3. DROP TABLE plan_12345.users CASCADE
4. DROP SCHEMA plan_12345 CASCADE (safety net, catches anything missed)
```

---

## Critical Analysis (Staff Engineer Review)

### Pros

| Aspect | Benefit |
|--------|---------|
| **Simplicity** | Single LIFO stack is easy to understand, implement, and debug |
| **LIFO ordering** | Natural dependency handling - last created, first deleted |
| **Schema isolation** | Each plan operates in its own namespace, no conflicts between concurrent tests |
| **Automatic cleanup** | Helper methods handle registration, test authors don't need to think about it |
| **Deterministic** | Same seed → same schema name → reproducible |
| **Safety net** | Final CASCADE schema drop catches any objects missed by individual cleanups |
| **Testable** | Dry-run mode, inspectable stack state |
| **Low overhead** | No background goroutines, no polling, just a slice append |

### Cons & Concerns

| Concern | Issue | Mitigation |
|---------|-------|------------|
| **Schema creation overhead** | Extra DDL per test plan | Minimal cost, schemas are lightweight |
| **Cluster settings are global** | Can't isolate to schema, affects all connections | Restore original value; accept this limitation |
| **Concurrent step access** | Multiple goroutines may push simultaneously | Mutex protects stack (already in design) |
| **Cleanup failure** | Individual DROP may fail (e.g., object already dropped) | `ContinueOnError: true` + `IF EXISTS` clauses |
| **Long cleanup times** | Many objects = many DROP statements | Batch where possible; CASCADE reduces count |
| **No crash recovery** | In-memory stack lost on process crash | Accept for roachtests; for DRT, persist stack |
| **Workload tables** | Workloads (TPCC/YCSB) create tables outside our helper | Either: (a) wrap workload init, or (b) rely on schema CASCADE |
| **Cross-plan dependencies** | One plan's object used by another | Not supported - each plan is isolated by design |
| **Original value capture** | Reading current setting value adds latency | Acceptable; one read per setting |

### Edge Cases

#### 1. Nested/Recursive Cleanup
```go
// What if cleanup statement itself causes side effects?
h.CreateTable("audit_log", "...")  // Registers DROP
// Later, a trigger on users creates audit entries
// DROP TABLE users → trigger fires → writes to audit_log
// DROP TABLE audit_log → works fine (LIFO handles this)
```
**Verdict**: LIFO ordering handles this correctly.

#### 2. Partial Execution Failure
```go
// Step 1: Creates table_a, table_b  → Pushes DROP a, DROP b
// Step 2: Fails midway
// Cleanup runs: DROP b, DROP a (both exist, both cleaned)
```
**Verdict**: Correct behavior.

#### 3. Step Creates Then Drops
```go
func step(h *Helper) error {
    name, _ := h.CreateTable("temp", "...")  // Pushes DROP temp
    // ... use table ...
    h.Exec("DROP TABLE " + name)  // Manual drop
    return nil
}
// Cleanup: DROP TABLE IF EXISTS temp → no-op (already dropped)
```
**Verdict**: `IF EXISTS` makes this safe.

#### 4. Concurrent Steps Create Objects
```go
// Steps A and B run concurrently
// A: CreateTable("x") → Push DROP x
// B: CreateTable("y") → Push DROP y
// Stack: [schema, x, y] or [schema, y, x] depending on timing
```
**Verdict**: Order between x and y doesn't matter (no dependencies). Mutex ensures no corruption.

#### 5. Workload Creates Tables
```go
// TPCC creates tpcc.warehouse, tpcc.order, etc.
// These are NOT in our plan schema, NOT registered in our stack
```
**Options**:
- (a) Modify workload to use plan schema (invasive)
- (b) Wrap workload in operation that registers cleanup
- (c) Accept workload manages its own lifecycle (current behavior)
- (d) Add explicit `h.RegisterExternalCleanup("DROP DATABASE tpcc CASCADE")`

**Recommendation**: Option (d) - provide escape hatch for external objects.

---

## Additional API: External Cleanup Registration

For objects created outside helper methods (e.g., workloads):

```go
// RegisterCleanup manually registers a cleanup statement.
// Use for objects created by external tools (workloads, etc.)
func (h *Helper) RegisterCleanup(description, statement string) {
    h.gc.Stack().PushSQL(description, statement)
}

// Usage in TPCC operation
func initTPCC(ctx context.Context, l *logger.Logger, h *Helper) error {
    // Workload creates tpcc database
    err := runTPCCInit(ctx, h.cluster)
    if err != nil {
        return err
    }

    // Manually register cleanup
    h.RegisterCleanup("Drop TPCC database", "DROP DATABASE IF EXISTS tpcc CASCADE")
    return nil
}
```

---

## Configuration Options

```go
type GCConfig struct {
    // Maximum time for cleanup execution
    Timeout time.Duration  // default: 5m

    // Continue cleanup if individual statements fail
    ContinueOnError bool   // default: true

    // Actually execute cleanup (false = log only)
    Execute bool           // default: true

    // Schema prefix for plan schemas
    SchemaPrefix string    // default: "plan_"

    // Database for plan schemas
    Database string        // default: "defaultdb"
}
```

---

## Observability

### Logging
```
[GC] Creating plan schema: plan_1234567890
[GC] Registered cleanup: Drop table plan_1234567890.users
[GC] Registered cleanup: Drop index users_name_idx
[GC] Registered cleanup: Restore cluster setting kv.snapshot... to '32 MiB'
[GC] Starting cleanup, 4 entries in stack
[GC] Cleanup [1]: Restore cluster setting kv.snapshot... to '32 MiB'
[GC]   OK
[GC] Cleanup [2]: Drop index users_name_idx
[GC]   OK
[GC] Cleanup [3]: Drop table plan_1234567890.users
[GC]   OK
[GC] Cleanup [4]: Drop plan schema plan_1234567890
[GC]   OK
[GC] Cleanup complete: 4 executed, 0 errors
```

### Stack Inspection (for debugging)
```go
// Dump current stack state
entries := h.gc.Stack().Entries()
for i, e := range entries {
    l.Printf("Stack[%d]: %s", i, e.Description)
}
```

---

## Summary

This design provides:

1. **Plan-scoped isolation** via unique schemas
2. **Automatic cleanup registration** via helper methods
3. **Correct dependency ordering** via LIFO execution
4. **Resilient cleanup** via `IF EXISTS` and `ContinueOnError`
5. **Safety net** via final schema CASCADE drop
6. **Escape hatch** via `RegisterCleanup` for external objects
7. **Observability** via comprehensive logging
8. **Testability** via dry-run mode and stack inspection

The main trade-off is that cluster settings remain global (can't be scoped to schema), but capturing and restoring original values mitigates this adequately for test scenarios.