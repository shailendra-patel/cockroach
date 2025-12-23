package modular

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

type TestOption func(options *TestOptions)
type debugModule string

const (
	ClusterStateDebug debugModule = "cluster-state"
	RunnerDebug       debugModule = "runner"
	PlannerDebug      debugModule = "planner"
	GCDebug           debugModule = "gc"
)

type debugModules map[debugModule]bool

func (d debugModules) NewLogger(parent *logger.Logger, module debugModule) *logger.Logger {
	var newLogger *logger.Logger
	if d != nil && d[module] {
		// Create a non-quiet logger that logs to modular_debug.txt
		newLogger, _ = parent.ChildLogger(string(module))
	} else {
		// Create a quiet logger that still logs to file but not to stdout/stderr
		newLogger, _ = parent.ChildLogger(string(module), logger.QuietStdout, logger.QuietStderr)
	}
	return newLogger
}

// WithDebug enables debug logging for specific modules.
func WithDebug(modules ...debugModule) TestOption {
	return func(options *TestOptions) {
		if options.debugModules == nil {
			options.debugModules = make(map[debugModule]bool)
		}
		for _, module := range modules {
			options.debugModules[module] = true
		}
	}
}

// WithGCEnabled enables or disables garbage collection for the test.
// GC is enabled by default. When enabled:
// - A plan-scoped schema (test_plan_{seed}) is created for object isolation
// - Global objects (cluster settings, zone configs) are tracked for restoration
// - Cleanup runs on both success and failure
func WithGCEnabled(enabled bool) TestOption {
	return func(options *TestOptions) {
		options.gcConfig.Enabled = enabled
	}
}

// WithGCDryRun configures the GC to log cleanup statements without executing them.
// Useful for debugging and testing.
func WithGCDryRun() TestOption {
	return func(options *TestOptions) {
		options.gcConfig.Execute = false
	}
}

// WithGCTimeout sets the maximum time allowed for cleanup execution.
func WithGCTimeout(timeout time.Duration) TestOption {
	return func(options *TestOptions) {
		options.gcConfig.Timeout = timeout
	}
}

// StageOption configures a Stage.
type StageOption func(*Stage)

// WithStepConcurrency sets the maximum number of steps that can run concurrently in a stage.
func WithStepConcurrency(concurrency int) StageOption {
	return func(stage *Stage) {
		stage.maxStepConcurrency = concurrency
	}
}

// StepOption configures a SingleStep.
type StepOption func(*SingleStep)

// InBackground configures a step to run in the background.
func InBackground() StepOption {
	return func(step *SingleStep) {
		step.background = make(shouldStop)
	}
}

func DisableConcurrency() StepOption {
	return func(step *SingleStep) {
		step.concurrencyDisabled = true
	}
}
