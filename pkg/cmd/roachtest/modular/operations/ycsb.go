package operations

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/modular"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

// YCSBWorkloadType represents the different YCSB workload types
type YCSBWorkloadType string

const (
	YCSBWorkloadA YCSBWorkloadType = "A" // Update heavy
	YCSBWorkloadB YCSBWorkloadType = "B" // Read mostly
	YCSBWorkloadC YCSBWorkloadType = "C" // Read only
	YCSBWorkloadD YCSBWorkloadType = "D" // Read latest
	YCSBWorkloadE YCSBWorkloadType = "E" // Short ranges
	YCSBWorkloadF YCSBWorkloadType = "F" // Read-modify-write
)

// YCSBOptions contains configuration for YCSB operations
type YCSBOptions struct {
	// Database name (defaults to "ycsb")
	Database string
	// Insert count for initialization (defaults to 1000000)
	InsertCount int
	// Concurrency level (defaults to optimal for workload type)
	Concurrency int
	// Duration of workload run (defaults to 30m)
	Duration time.Duration
	// Ramp time before starting measurements (defaults to 2m)
	RampTime time.Duration
	// Read committed isolation level
	ReadCommitted bool
	// Uniform request distribution instead of zipfian
	UniformDistribution bool
	// Extra arguments for workload command
	ExtraArgs string
}

// YCSBOp implements the Operation interface for YCSB workload operations
type YCSBOp struct {
	name       string
	builder    *modular.OperationBuilder
	workload   YCSBWorkloadType
	initOnly   bool
	runOnly    bool
	opts       YCSBOptions
}

// Chain returns the operation's chain of steps
func (y *YCSBOp) Chain() modular.Chain {
	return y.builder.Chain
}

// Name returns the operation's name
func (y *YCSBOp) Name() string {
	return y.name
}

func (y *YCSBOp) Precondition() bool {
	return true
}

func (y *YCSBOp) Timeout() time.Duration {
	// Add buffer time for setup + run duration
	return y.opts.Duration + y.opts.RampTime + 30*time.Minute
}

// YCSB creates a complete YCSB operation that initializes and runs a workload
func YCSB(c cluster.Cluster, workload YCSBWorkloadType, opts YCSBOptions) modular.Operation {
	return createYCSBOp(c, workload, false, false, opts)
}

// YCSBInit creates an operation that only initializes the YCSB workload (no run)
func YCSBInit(c cluster.Cluster, opts YCSBOptions) modular.Operation {
	return createYCSBOp(c, YCSBWorkloadA, true, false, opts)
}

// YCSBRun creates an operation that only runs a YCSB workload (assumes already initialized)
func YCSBRun(c cluster.Cluster, workload YCSBWorkloadType, opts YCSBOptions) modular.Operation {
	return createYCSBOp(c, workload, false, true, opts)
}

func createYCSBOp(c cluster.Cluster, workload YCSBWorkloadType, initOnly, runOnly bool, opts YCSBOptions) modular.Operation {
	// Set defaults
	if opts.Database == "" {
		opts.Database = "ycsb"
	}
	if opts.InsertCount == 0 {
		opts.InsertCount = 1000000
	}
	if opts.Duration == 0 {
		opts.Duration = 30 * time.Minute
	}
	if opts.RampTime == 0 {
		opts.RampTime = 2 * time.Minute
	}
	if opts.Concurrency == 0 {
		// Default concurrency based on workload type (simplified)
		concurrencyMap := map[YCSBWorkloadType]int{
			YCSBWorkloadA: 144,
			YCSBWorkloadB: 192,
			YCSBWorkloadC: 192,
			YCSBWorkloadD: 144,
			YCSBWorkloadE: 144,
			YCSBWorkloadF: 144,
		}
		opts.Concurrency = concurrencyMap[workload]
	}

	var builder *modular.OperationBuilder

	if initOnly {
		builder = modular.NewOperation("initialize ycsb workload", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
			return runYCSBCommand(ctx, l, c, workload, opts, true, false)
		})
	} else if runOnly {
		builder = modular.NewOperation(fmt.Sprintf("run ycsb workload %s", workload), func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
			return runYCSBCommand(ctx, l, c, workload, opts, false, true)
		})
	} else {
		builder = modular.NewOperation(fmt.Sprintf("initialize ycsb workload %s", workload), func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
			return runYCSBCommand(ctx, l, c, workload, opts, true, false)
		}).Then(fmt.Sprintf("run ycsb workload %s", workload), func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
			return runYCSBCommand(ctx, l, c, workload, opts, false, true)
		})
	}

	var opType string
	if initOnly {
		opType = "init"
	} else if runOnly {
		opType = "run"
	} else {
		opType = "full"
	}

	return &YCSBOp{
		name:     fmt.Sprintf("ycsb-%s/%s/concurrency=%d", opType, workload, opts.Concurrency),
		builder:  builder,
		workload: workload,
		initOnly: initOnly,
		runOnly:  runOnly,
		opts:     opts,
	}
}

func runYCSBCommand(ctx context.Context, l *logger.Logger, c cluster.Cluster, workload YCSBWorkloadType, opts YCSBOptions, init, run bool) error {
	var cmd string

	if init && run {
		// Both init and run in one command
		cmd = fmt.Sprintf("./cockroach workload run ycsb --init --insert-count=%d --workload=%s --concurrency=%d --splits=%d",
			opts.InsertCount, workload, opts.Concurrency, len(c.CRDBNodes()))
	} else if init {
		// Initialize only
		cmd = fmt.Sprintf("./cockroach workload init ycsb --insert-count=%d --splits=%d",
			opts.InsertCount, len(c.CRDBNodes()))
	} else if run {
		// Run only
		cmd = fmt.Sprintf("./cockroach workload run ycsb --workload=%s --concurrency=%d --duration=%s",
			workload, opts.Concurrency, opts.Duration)
	}

	// Add common options
	if opts.RampTime > 0 && run {
		cmd += fmt.Sprintf(" --ramp=%s", opts.RampTime)
	}
	if opts.ReadCommitted {
		cmd += " --isolation-level=read_committed"
	}
	if opts.UniformDistribution {
		cmd += " --request-distribution=uniform"
	}
	if opts.ExtraArgs != "" {
		cmd += " " + opts.ExtraArgs
	}

	// Add database specification
	cmd += fmt.Sprintf(" {pgurl%s}", c.CRDBNodes())

	l.Printf("Running YCSB command: %s", cmd)
	return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd)
}