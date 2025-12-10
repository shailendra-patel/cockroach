# Modular Test Framework - Architecture Summary

This document provides a technical deep-dive into the modular test framework for CockroachDB roachtests. It summarizes the framework's design, components, and execution model.

## Overview

The modular test framework is a declarative, DAG-based test execution system that enables:

1. **Exploratory Testing**: Randomly generates valid execution orderings to discover timing/concurrency bugs
2. **Composable Operations**: Reusable test operations that can be combined across tests
3. **State Tracking**: Automatic tracking of cluster state changes for restoration on failure
4. **Unified Framework**: Bridges roachtests and DRT operations into a single underlying system

## Core Abstractions

### Hierarchy (Bottom to Top)

```
testStep → stepGroup → Chain → Stage → Test → TestPlanner → TestPlan → Runner
```

### Step (`step.go`)

The atomic unit of work. A step runs from start to finish without being preempted.

```go
type StepProtocol interface {
    Description() string
    Run(context.Context, *logger.Logger, *Helper) error
}

type SingleStep struct {
    description         string
    fn                  stepFunc  // func(ctx, logger, helper) error
    background          shouldStop
    concurrencyDisabled bool
}
```

**Key Types:**
- `SingleStep`: Basic test step executing a single function
- `concurrentStep`: Meta-step grouping multiple steps for parallel execution

### stepGroup (`graph.go`)

A collection of `testStep`s that share the same dependencies and can run concurrently or interchangeably.

```go
type stepGroup []testStep
```

### Chain (`graph.go`)

A sequence of step groups with strict ordering. Steps within the same chain must execute in depth order, but can interleave with steps from other chains.

```go
type Chain []stepGroup  // alias: chain
```

### Stage (`graph.go`)

Groups one or more chains that all converge at stage boundaries. All chains in a stage must complete before the next stage begins.

```go
type Stage struct {
    name                     string
    index                    int
    chains                   []chain
    failureInjectionDisabled bool
    maxStepConcurrency       int
}
```

### Test (`test.go`)

The top-level test definition containing setup, main stages, and after-test stages.

```go
type Test struct {
    setupStage     *Stage      // Sequential setup, no failure injection
    stages         []*Stage    // Main test stages
    afterTestStage *Stage      // Cleanup stage
    options        TestOptions
    // ... context, cluster, logger
}
```

## Execution Pipeline

### 1. Test Definition (via StepBuilder)

Tests are defined using a fluent API:

```go
mt := modular.NewTest(ctx, logger, cluster, crdbNodes)

// Setup stage (sequential, no failure injection)
mt.Setup("import data", importFunc)

// Main test stage
testStage := mt.NewStage("test", modular.WithStepConcurrency(3))

// Chain 1: TPCC workload
mt.InStage(testStage, "init TPCC", initTPCC).
    Then("run TPCC", runTPCC).
    Then("check consistency", checkConsistency)

// Chain 2: Replication changes (can interleave with Chain 1)
mt.InStage(testStage, "increase RF", increaseRF).
    Then("wait for replication", waitReplicate).
    Then("decrease RF", decreaseRF)

// After-test cleanup
mt.AfterTest("validate data", validateFunc)
```

**StepBuilder Methods:**
- `InStage(stage, name, fn)`: Start a new chain in a stage
- `Then(name, fn)`: Add sequential step (new stepGroup)
- `And(name, fn)`: Add parallel step (same stepGroup)

### 2. DAG Construction (`test.go`, `step_builder.go`)

When `NewPlanner()` is called:
1. Failure injection operations are added to stages (TODO: not fully implemented)
2. Step positions are assigned via `AssignStepOrder()`
3. Stages are combined (setup + main + after-test)

```go
func (t *Test) NewPlanner() TestPlanner {
    for stageIdx := range t.stages {
        t.AddFailureInjection(t.stages[stageIdx])
    }
    // Assign step positions (chainID, depth)
    for stageIdx := range t.stages {
        AssignStepOrder(t.stages[stageIdx])
    }
    // ...
}
```

### 3. Plan Generation (`planner.go`)

The `TestPlanner` linearizes the DAG into an executable `TestPlan`:

```go
func (p *TestPlanner) Plan() (*TestPlan, error) {
    for _, stage := range p.stages {
        plan := p.generateStagePlan(stage)      // Random linearization
        p.CreateConcurrentSteps(&plan)          // Random concurrent grouping
        stagePlans = append(stagePlans, plan)
    }
    // ...
}
```

**Randomization Algorithm:**
1. Start with a valid ordering (all steps in chain 0, then chain 1, etc.)
2. Propose n^3*log(n) random adjacent swaps
3. Accept swap if steps are in different chains OR same depth (stepGroup)
4. Randomly group steps into concurrent batches (respecting maxStepConcurrency)

**Validity Constraints:**
- Steps in the same chain must maintain depth ordering
- Steps with `concurrencyDisabled` cannot be grouped
- Concurrent groups respect `maxStepConcurrency` limit

### 4. Plan Execution (`runner.go`)

The `Runner` executes the linearized plan:

```go
func (r *Runner) executeSteps(ctx context.Context, l *logger.Logger) error {
    for stageIdx, stagePlan := range r.testPlan.stagePlans {
        for _, step := range stagePlan.steps {
            err = step.Run(ctx, stepLogger, r.helper)
            // Handle errors, log state...
        }
    }
}
```

**Features:**
- Per-step and per-stage logging
- State tracking after each step
- Automatic cleanup on failure (if enabled)
- Log file renaming on failure (prefixed with `FAILED_`)

## State Tracking (`state_tracker.go`)

The `ClusterStateTracker` records all cluster modifications for potential rollback:

```go
type ClusterStateTracker struct {
    tablesAdded       map[string]struct{}
    clusterSettings   sync.Map  // setting -> original value
    zoneConfigs       sync.Map  // range -> original config
    failuresInjected  map[string]*failures.Failer
    schemasCreated    map[string]struct{}
    usersCreated      map[string]struct{}
    databasesCreated  map[string]struct{}
    indexesCreated    map[string]struct{}
}
```

**Tracked Operations:**
- Tables, databases, schemas, users, indexes created
- Cluster settings modified (stores original values)
- Zone configurations modified (stores original configs)
- Failure injections (for recovery)

## Helper API (`helper.go`)

The `Helper` provides test steps with cluster interaction capabilities:

### Database Operations
```go
h.Connect(node int) *gosql.DB
h.RandomDBConn() *gosql.DB
h.Query(query string, args...) (*gosql.Rows, error)
h.QueryRow(query string, args...) *gosql.Row
h.Exec(query string, args...) error
```

### State-Tracked Operations
```go
h.CreateTable(prefix, schema string) (string, error)
h.CreateDatabase(prefix string, args...) (string, error)
h.CreateSchema(prefix string, args...) (string, error)
h.CreateIndex(prefix, db, table string, columns []string, args...) (string, error)
h.CreateUser(prefix string, args...) (string, error)
h.SetClusterSetting(name, value string) error
h.AlterRange(rangeName, zoneConfig string) error
h.AlterAllRanges(zoneConfig string) error
```

### Utility Functions
```go
h.AvailableNodes() option.NodeListOption
h.RandomAvailableNode() int
h.PickRandomDatabase() (string, error)
h.PickRandomTable(database string) (string, error)
h.GetTableColumns(database, table string) ([]ColumnInfo, error)
h.SearchTable(pred func(db, table string) bool) (string, string, error)
```

### Background Tasks
```go
h.Go(fn task.Func, opts...)
h.GoWithCancel(fn task.Func, opts...) context.CancelFunc
h.NewGroup(opts...) task.Group
h.NewErrorGroup(opts...) task.ErrorGroup
h.GoCommand(cmd string, nodes option.NodeListOption) context.CancelFunc
```

## Operations (`operations/` package)

Reusable, composable test operations that implement the `Operation` interface:

```go
type Operation interface {
    Chain() Chain
    Name() string
    Precondition() bool
    Timeout() time.Duration
}
```

### Available Operations

| Operation | File | Description |
|-----------|------|-------------|
| `TPCC` | `tpcc.go` | TPC-C workload (init → run → check) |
| `YCSB` | `ycsb.go` | YCSB workload (A-F variants) |
| `ReplicationFactorCycle` | `replication.go` | Increase RF to 5, wait, decrease to 3 |
| `AddRandomIndex` | `add_index.go` | Add random index to random table |
| Schema change helpers | `schema_change.go` | AddColumn, DropColumn, CreateTable, etc. |

### Operation Builder Pattern

```go
builder := modular.NewOperation("step 1", step1Func).
    Then("step 2", step2Func).
    MaybeThen(condition, "optional step", optionalFunc).
    And("parallel step", parallelFunc)

return &MyOp{builder: builder, ...}
```

### Adding Operations to Tests

```go
tpccOp := operations.TPCC(cluster, warehouses, duration, opts)
mt.AddOperation(testStage, tpccOp)
```

## Configuration Options

### Test Options (`option.go`)

```go
modular.WithDebug(ClusterStateDebug, RunnerDebug, PlannerDebug)
modular.CleanupOnFailure()
```

### Stage Options

```go
modular.WithStepConcurrency(maxConcurrent int)
```

### Step Options

```go
modular.InBackground()        // Run step in background
modular.DisableConcurrency()  // Prevent step from being grouped
```

## DAG Visualization (`dag_builder.go`)

The framework generates ASCII DAG visualizations for debugging:

```
                       [setup]
              ┌───────────────────┐
              │  importing tpcc   │
              │     workload      │
              └───────────────────┘
                        │
                     [test]
          ┼───────────────────────────┼
          ▼                           ▼
┌───────────────────┐     ┌───────────────────┐
│   running TPCC    │     │    increasing     │
│workload for 1 hour│     │replication factor │
└───────────────────┘     └───────────────────┘
          │                         │
          ▼                         ▼
┌───────────────────┐     ┌───────────────────┐
│   dropping TPCC   │     │    waiting for    │
│      tables       │     │    replication    │
└───────────────────┘     └───────────────────┘
```

## Testing Infrastructure

### Property-Based Tests (`planner_test.go`)

- **`TestDependencyOrdering`**: Validates that generated plans never violate chain ordering
- **`TestPlanDistribution`**: Chi-square test ensuring uniform distribution across legal permutations
- **`TestConcurrencyDistribution`**: Validates uniform distribution of concurrent groupings

### Test Data (`testdata/`)

- `basic_dag.txt`: Complex multi-chain DAG visualization
- `mvt_dag.txt`: Mixed-version test DAG example
- `and_dag.txt`: DAG with parallel steps
- `marshal_json.txt`: JSON serialization test

## Key Design Decisions

1. **Position-Based Ordering**: Each step has a `stepPosition{chainID, depth}` that enables O(1) swap validity checks

2. **Randomization Strategy**: Uses n^3*log(n) random adjacent swaps, which provably mixes to uniform distribution

3. **Separate Linearization and Grouping**: First generates a valid step ordering, then randomly groups into concurrent batches

4. **Atomic State Tracking**: Uses `sync.Map` for cluster settings/zone configs to handle concurrent access

5. **Original Value Preservation**: Only stores the first observed value for settings (before any test modifications)

## File Structure Summary

```
modular/
├── BUILD.bazel
├── README.md              # User-facing documentation
├── ARCHITECTURE.md        # This file
├── graph.go               # Stage, Chain, stepGroup, testStep definitions
├── step.go                # StepProtocol, SingleStep, concurrentStep
├── operation.go           # Operation interface, registry
├── step_builder.go        # Fluent API for test construction
├── test.go                # Test definition, NewPlanner()
├── planner.go             # TestPlanner, TestPlan, randomization
├── runner.go              # Execution engine
├── state_tracker.go       # ClusterStateTracker
├── helper.go              # Helper, Service for step interactions
├── option.go              # Configuration options
├── dag_builder.go         # ASCII DAG visualization
├── dag_builder_test.go
├── planner_test.go        # Property-based tests
├── json_test.go
├── operations/            # Reusable operations
│   ├── add_index.go
│   ├── inspect.go
│   ├── replication.go
│   ├── schema_change.go
│   ├── tpcc.go
│   └── ycsb.go
└── testdata/              # Test fixtures
    ├── basic_dag.txt
    ├── mvt_dag.txt
    └── ...
```

## Future Work (TODOs in Code)

1. **Failure Injection**: `AddFailureInjection()` is stubbed - needs implementation
2. **Dynamic Timeouts**: Operation timeouts should be calculated dynamically
3. **Background Steps**: `InBackground()` option exists but integration incomplete
4. **Operation Registry**: Global registry exists but not fully utilized