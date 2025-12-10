package modular

import (
	"context"
	gosql "database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

var (
	// everything that is not an alphanum or a few special characters
	invalidChars = regexp.MustCompile(`[^a-zA-Z0-9 \-_.]`)
)

// Runner executes a generated test plan from the modular framework.
type Runner struct {
	testPlan     *TestPlan
	helper       *Helper
	stateTracker *ClusterStateTracker
	gc           *GarbageCollector
}

// NewRunner creates a new runner for executing a test plan.
func NewRunner(testPlan *TestPlan) *Runner {
	clusterStateLogger := testPlan.debugModules.NewLogger(testPlan.logger, ClusterStateDebug)
	gcLogger := testPlan.debugModules.NewLogger(testPlan.logger, GCDebug)

	// Create GC with connection function (will be set up properly in initializeHelper)
	// For now, create with a placeholder that will be replaced
	gc := NewGarbageCollector(
		testPlan.seed,
		nil, // connFunc will be set in initializeHelper
		gcLogger,
		testPlan.gcConfig,
	)

	return &Runner{
		testPlan:     testPlan,
		helper:       &Helper{rng: testPlan.rng},
		stateTracker: NewClusterStateTracker(clusterStateLogger),
		gc:           gc,
	}
}

// RunTestPlan executes the test plan using the provided roachtest.Test interface.
// It logs the DAG and test plan, then executes all steps in position.
func RunTestPlan(ctx context.Context, t test.Test, testPlan *TestPlan) error {
	runner := NewRunner(testPlan)
	return runner.Run(ctx, t)
}

// RunPlan is a convenience function that executes a sequence of Operations directly.
// This is useful for simple test scenarios that don't need the full test planning
// infrastructure (stages, setup, after-test hooks, etc.).
func RunPlan(ctx context.Context, l *logger.Logger, c cluster.Cluster, operations []Operation) error {
	for i, op := range operations {
		l.Printf("Running operation %d/%d: %s", i+1, len(operations), op.Name())

		// Create a minimal helper for running the operation
		helper := &Helper{
			ctx:    ctx,
			logger: l,
		}

		// Set up cluster connection if available
		if c != nil {
			helper.cluster = c
			helper.defaultService = &Service{
				name:       install.SystemInterfaceName,
				ctx:        ctx,
				stepLogger: l,
				cluster:    c,
				nodes:      c.CRDBNodes(),
				connFunc: func(node int) *gosql.DB {
					return c.Conn(ctx, l, node)
				},
			}
		}

		// Execute each step in the operation's chain
		chain := op.Chain()
		for stepIdx, stepGroup := range chain {
			for _, step := range stepGroup {
				stepLogger, err := l.ChildLogger(fmt.Sprintf("%d_%s_%d", i, op.Name(), stepIdx))
				if err != nil {
					stepLogger = l // Fall back to parent logger
				}

				if err := step.Run(ctx, stepLogger, helper); err != nil {
					return fmt.Errorf("operation %s step %d failed: %w", op.Name(), stepIdx, err)
				}
			}
		}

		l.Printf("Completed operation %d/%d: %s", i+1, len(operations), op.Name())
	}

	return nil
}

// Run executes the test plan, logging the DAG and test plan before execution.
func (r *Runner) Run(ctx context.Context, t test.Test) error {
	l := t.L()

	// Phase 0: Ensure the cluster is running before any initialization.
	// This is a temporary fix - ideally tests should explicitly start the cluster
	// in a Setup step, but for now we auto-start if needed.
	if r.testPlan.cluster != nil {
		if err := r.ensureClusterRunning(ctx, t); err != nil {
			return fmt.Errorf("failed to ensure cluster is running: %w", err)
		}
	}

	// Phase 1: Set up basic GC connection (without search_path) and create schema
	// This must happen BEFORE we set up helper connections with search_path
	if r.gc.Enabled() && r.testPlan.cluster != nil {
		// Set up a basic connection for schema creation (no search_path yet)
		r.gc.connFunc = func() *gosql.DB {
			return r.testPlan.cluster.Conn(ctx, t.L(), 1)
		}

		// Create the plan schema
		if err := r.gc.Initialize(ctx); err != nil {
			return fmt.Errorf("failed to initialize GC: %w", err)
		}
	}

	// Phase 2: Initialize helper with connections that use the plan schema
	// Now that schema exists, connections can safely set search_path to it
	r.initializeHelper(ctx, t)

	// Always run cleanup at the end (success or failure)
	defer func() {
		if r.gc.Enabled() {
			l.Printf("Executing cleanup...")
			if err := r.gc.ExecuteCleanup(ctx); err != nil {
				l.Printf("Cleanup completed with errors: %v", err)
			}
		}
	}()

	// Log the test plan details
	l.Printf("Seed: %d", r.testPlan.seed)
	l.Printf("Number of stages: %d", len(r.testPlan.stagePlans))

	// Log the full test plan structure
	l.Printf("Test Plan Structure:\n%s", r.testPlan.String())

	// Generate and log the DAG
	planner := &TestPlanner{
		seed:   r.testPlan.seed,
		stages: r.extractStages(),
	}
	dag := planner.DAG()
	l.Printf("DAG Visualization:\n%s", dag)

	// Execute all steps in the plan
	return r.executeSteps(ctx, l)
}

// initializeHelper sets up the helper with the necessary context and dependencies.
func (r *Runner) initializeHelper(ctx context.Context, t test.Test) {
	// Initialize helper with context and cluster information
	r.helper.ctx = ctx
	r.helper.logger = t.L()
	r.helper.stateTracker = r.stateTracker
	r.helper.gc = r.gc

	// Use the test's task management interface
	r.helper.background = &testTaskManager{test: t}

	// Get cluster information from the test plan
	var c cluster.Cluster
	var crdbNodes option.NodeListOption
	if r.testPlan.cluster != nil {
		c = r.testPlan.cluster
		crdbNodes = r.testPlan.crdbNodes
		r.helper.cluster = c
	}

	// Set up connection function if cluster is available
	var connFunc func(int) *gosql.DB
	if c != nil {
		// Build connection options, including search_path if GC is enabled
		var connOpts []option.OptionFunc
		if r.gc.Enabled() {
			// Set search_path to plan schema so unqualified table names use it.
			// Note: We only include the plan schema here, not "public", because
			// the comma in "schema, public" causes parsing issues in connection options.
			// Public schema objects can be accessed with explicit qualification if needed.
			connOpts = append(connOpts, option.ConnectionOption("options", "-c search_path="+r.gc.Schema().Name))
		}

		connFunc = func(node int) *gosql.DB {
			return c.Conn(ctx, t.L(), node, connOpts...)
		}

		// Update GC with the connection function (uses node 1 by default for cleanup)
		r.gc.connFunc = func() *gosql.DB {
			return c.Conn(ctx, t.L(), 1, connOpts...)
		}
	}

	// Set up the default service with cluster functionality.
	// Use SystemInterfaceName ("system") so the monitor can track available nodes.
	r.helper.defaultService = &Service{
		name:       install.SystemInterfaceName,
		ctx:        ctx,
		connFunc:   connFunc,
		stepLogger: t.L(),
		monitor:    t.Monitor(),
		cluster:    c,
		nodes:      crdbNodes,
	}
}

// testTaskManager adapts the test.Test interface to the task.Manager interface
type testTaskManager struct {
	test test.Test
}

func (tm *testTaskManager) GoWithCancel(fn task.Func, opts ...task.Option) context.CancelFunc {
	return tm.test.GoWithCancel(fn, opts...)
}

func (tm *testTaskManager) Go(fn task.Func, opts ...task.Option) {
	tm.test.Go(fn, opts...)
}

func (tm *testTaskManager) NewGroup(opts ...task.Option) task.Group {
	return tm.test.NewGroup(opts...)
}

func (tm *testTaskManager) NewErrorGroup(opts ...task.Option) task.ErrorGroup {
	return tm.test.NewErrorGroup(opts...)
}

func (tm *testTaskManager) Terminate(_ *logger.Logger) {
	// The test framework handles termination
}

func (tm *testTaskManager) Cancel() {
	// The test framework handles cancellation
}

func (tm *testTaskManager) CompletedEvents() <-chan task.Event {
	// Return a closed channel since the test framework handles events
	ch := make(chan task.Event)
	close(ch)
	return ch
}

// extractStages extracts Stage objects from the test plan for DAG generation.
func (r *Runner) extractStages() []Stage {
	var stages []Stage
	for _, stagePlan := range r.testPlan.stagePlans {
		if stagePlan.stage != nil {
			stages = append(stages, *stagePlan.stage)
		}
	}
	return stages
}

// executeSteps executes all steps in the test plan sequentially by stage.
// Cleanup is handled by the defer in Run(), so no manual cleanup logic here.
func (r *Runner) executeSteps(ctx context.Context, l *logger.Logger) error {
	for stageIdx, stagePlan := range r.testPlan.stagePlans {
		stageName := stagePlan.stage.name
		if stageName == "" {
			stageName = fmt.Sprintf("stage %d", stageIdx+1)
		}

		stageLogger, err := r.loggerForStage(l, stageIdx, stageName)
		if err != nil {
			return fmt.Errorf("failed to create stage logger: %w", err)
		}

		r.logStage("STARTING", stageName, stageLogger)
		start := time.Now()

		err = r.executeStage(ctx, stageLogger, stagePlan)
		if err != nil {
			return r.stageError(ctx, err, stageName, stageLogger)
		}

		duration := time.Since(start)
		prefix := fmt.Sprintf("FINISHED [%s]", duration)
		r.logStage(prefix, stageName, stageLogger)

		r.stateTracker.LogClusterStateDebug(fmt.Sprintf("after stage: %s", stageName))
	}

	l.Printf("All stages completed successfully")
	return nil
}

// executeStage executes all steps within a single stage.
func (r *Runner) executeStage(ctx context.Context, l *logger.Logger, stagePlan stagePlan) error {
	for _, step := range stagePlan.steps {
		stepLogger, err := r.loggerForStep(l, step.stepID, step.Description())
		if err != nil {
			return fmt.Errorf("failed to create step logger: %w", err)
		}

		r.logStep("STARTING", step.stepID, step.Description(), stepLogger)
		start := time.Now()

		err = step.Run(ctx, stepLogger, r.helper)
		if err != nil {
			return r.stepError(ctx, err, step.stepID, step.Description(), stepLogger)
		}

		duration := time.Since(start)
		prefix := fmt.Sprintf("FINISHED [%s]", duration)
		r.logStep(prefix, step.stepID, step.Description(), stepLogger)

		// Print tracked state after each step for debugging and visibility
		stateOutput := r.stateTracker.PrintTrackedState()
		stepLogger.Printf("State after step completion:\n%s", stateOutput)
	}

	return nil
}

// logStage logs stage start/finish messages with consistent formatting.
func (r *Runner) logStage(prefix, stageName string, l *logger.Logger) {
	dashes := strings.Repeat("=", 10)
	l.Printf("%[1]s %s: %s %[1]s", dashes, prefix, stageName)
}

// logStep logs step start/finish messages with consistent formatting.
func (r *Runner) logStep(prefix string, stepID int, stepDesc string, l *logger.Logger) {
	dashes := strings.Repeat("-", 10)
	l.Printf("%[1]s %s (%d): %s %[1]s", dashes, prefix, stepID, stepDesc)
}

// loggerForStage creates a logger instance for a stage.
func (r *Runner) loggerForStage(parent *logger.Logger, stageIdx int, stageName string) (*logger.Logger, error) {
	name := invalidChars.ReplaceAllString(strings.ToLower(stageName), "")
	name = fmt.Sprintf("stage_%d_%s", stageIdx, name)
	return parent.ChildLogger(name)
}

// loggerForStep creates a logger instance for a step, similar to mixed-version runner.
func (r *Runner) loggerForStep(parent *logger.Logger, stepID int, stepDesc string) (*logger.Logger, error) {
	name := invalidChars.ReplaceAllString(strings.ToLower(stepDesc), "")
	name = fmt.Sprintf("%d_%s", stepID, name)
	return parent.ChildLogger(name)
}

// stepError generates a detailed error for step failures.
func (r *Runner) stepError(ctx context.Context, err error, stepID int, stepDesc string, l *logger.Logger) error {
	stepErr := fmt.Errorf("modular test failure while running step %d (%s): %w", stepID, stepDesc, err)

	// Log the error for convenience
	l.Printf("Step failed: %+v", stepErr)

	// Rename the log file to indicate failure
	if renameErr := r.renameFailedLogger(l); renameErr != nil {
		l.Printf("could not rename failed step logger: %v", renameErr)
	}

	return stepErr
}

// stageError generates a detailed error for stage failures.
func (r *Runner) stageError(ctx context.Context, err error, stageName string, l *logger.Logger) error {
	stageErr := fmt.Errorf("modular test failure while running stage %s: %w", stageName, err)

	// Log the error for convenience
	l.Printf("Stage failed: %+v", stageErr)

	// Rename the log file to indicate failure
	if renameErr := r.renameFailedLogger(l); renameErr != nil {
		l.Printf("could not rename failed stage logger: %v", renameErr)
	}

	return stageErr
}

// recoverFromFailures attempts to recover from all injected failures.
// This is called during cleanup to ensure the cluster is returned to a healthy state.
func (r *Runner) recoverFromFailures(ctx context.Context, l *logger.Logger) error {
	failureMap := r.stateTracker.GetTrackedFailures()
	if len(failureMap) == 0 {
		return nil
	}

	l.Printf("Recovering from %d injected failures...", len(failureMap))
	var lastErr error
	for failureID, failer := range failureMap {
		l.Printf("Recovering from failure %s: %s", failureID, failer.Description())
		if err := failer.Recover(ctx, l); err != nil {
			l.Printf("Failed to recover from failure %s: %v", failureID, err)
			lastErr = err
			// Continue with other recoveries
		}
	}

	if lastErr != nil {
		return fmt.Errorf("failure recovery completed with errors")
	}
	l.Printf("Failure recovery completed successfully")
	return nil
}

// renameFailedLogger renames the log file to include "FAILED" prefix.
func (r *Runner) renameFailedLogger(l *logger.Logger) error {
	if l.File == nil {
		return nil
	}

	currentFileName := l.File.Name()
	newLogName := filepath.Join(
		filepath.Dir(currentFileName),
		"FAILED_"+filepath.Base(currentFileName),
	)
	return os.Rename(currentFileName, newLogName)
}

// ensureClusterRunning checks if the cluster is running and starts it if not.
// This is a temporary fix to ensure the cluster is available before GC initialization.
// Ideally, tests should explicitly start the cluster in a Setup step.
func (r *Runner) ensureClusterRunning(ctx context.Context, t test.Test) error {
	c := r.testPlan.cluster
	l := t.L()

	// Try to connect to node 1 to check if cluster is running
	if r.isClusterRunning(ctx, c, l) {
		l.Printf("Cluster is already running")
		return nil
	}

	// Cluster is not running, start it with default settings
	l.Printf("Cluster is not running, starting with default settings...")
	c.Start(ctx, l, option.DefaultStartOpts(), install.MakeClusterSettings(), r.testPlan.crdbNodes)

	// Verify the cluster started successfully
	if !r.isClusterRunning(ctx, c, l) {
		return fmt.Errorf("failed to start cluster: unable to connect after start")
	}

	l.Printf("Cluster started successfully")
	return nil
}

// isClusterRunning attempts to connect to the cluster and run a simple query.
func (r *Runner) isClusterRunning(ctx context.Context, c cluster.Cluster, l *logger.Logger) bool {
	// Use a short timeout for the connection check
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	db, err := c.ConnE(checkCtx, l, 1)
	if err != nil {
		return false
	}
	defer db.Close()

	// Try a simple query to verify the connection works
	var result int
	err = db.QueryRowContext(checkCtx, "SELECT 1").Scan(&result)
	return err == nil
}
