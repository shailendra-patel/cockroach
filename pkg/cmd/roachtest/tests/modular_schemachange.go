package tests

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/modular"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/modular/operations"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func registerModularSchemaChange(r registry.Registry) {
	// Light workload with many schema changes
	r.Add(registry.TestSpec{
		Name:    "modular-schemachange/light-workload-heavy-schema",
		Owner:   registry.OwnerSQLFoundations,
		Cluster: r.MakeClusterSpec(4, spec.CPU(16), spec.WorkloadNode()),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runSchemaChangeDuringTPCC(ctx, t, c, schemaChangeConfig{
				warehouses:   1, // Light workload
				duration:     10 * time.Minute,
				numSchemaOps: 8, // Many schema changes
				randomSeed:   0, // Use time-based seed
				minDelay:     10 * time.Second,
				maxDelay:     2 * time.Minute,
			})
		},
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Nightly),
	})

	// Heavy workload with few schema changes
	r.Add(registry.TestSpec{
		Name:    "modular-schemachange/heavy-workload-light-schema",
		Owner:   registry.OwnerSQLFoundations,
		Cluster: r.MakeClusterSpec(4, spec.CPU(32), spec.WorkloadNode()),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runSchemaChangeDuringTPCC(ctx, t, c, schemaChangeConfig{
				warehouses:   50, // Heavy workload
				duration:     20 * time.Minute,
				numSchemaOps: 3, // Few schema changes
				randomSeed:   0,
				minDelay:     2 * time.Minute,
				maxDelay:     5 * time.Minute,
			})
		},
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Nightly),
	})

	// Stress test with many random schema changes
	r.Add(registry.TestSpec{
		Name:    "modular-schemachange/stress-random",
		Owner:   registry.OwnerSQLFoundations,
		Cluster: r.MakeClusterSpec(4, spec.CPU(16), spec.WorkloadNode()),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runSchemaChangeDuringTPCC(ctx, t, c, schemaChangeConfig{
				warehouses:   10,
				duration:     15 * time.Minute,
				numSchemaOps: 12, // Many random changes
				randomSeed:   0,
				minDelay:     5 * time.Second,
				maxDelay:     90 * time.Second,
			})
		},
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Nightly),
	})

	// Reproducible test with fixed seed
	r.Add(registry.TestSpec{
		Name:    "modular-schemachange/reproducible",
		Owner:   registry.OwnerSQLFoundations,
		Cluster: r.MakeClusterSpec(4, spec.CPU(16), spec.WorkloadNode()),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runSchemaChangeDuringTPCC(ctx, t, c, schemaChangeConfig{
				warehouses:   5,
				duration:     8 * time.Minute,
				numSchemaOps: 6,
				randomSeed:   12345, // Fixed seed for reproducibility
				minDelay:     15 * time.Second,
				maxDelay:     90 * time.Second,
			})
		},
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Nightly),
	})
}

type schemaChangeConfig struct {
	warehouses   int
	duration     time.Duration
	numSchemaOps int
	randomSeed   int64 // 0 = use time-based seed
	minDelay     time.Duration
	maxDelay     time.Duration
}

func runSchemaChangeDuringTPCC(ctx context.Context, t test.Test, c cluster.Cluster, cfg schemaChangeConfig) {
	// Start cluster
	c.Start(ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule), install.MakeClusterSettings(), c.CRDBNodes())

	// Setup database
	setupOp := modular.NewOperation("setup database", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		db := c.Conn(ctx, l, 1)
		defer db.Close()
		return enableIsolationLevels(ctx, t, db)
	}).Build("setup database")

	// Initialize and run TPCC workload
	tpccOp := operations.TPCC(c, cfg.warehouses, cfg.duration, operations.TPCCExtraOptions{
		// This will both initialize the TPCC schema AND run the workload
	})

	// Create available schema change operations
	allSchemaOps := createSchemaChangeOperations("tpcc")

	// Setup randomization
	var rng *rand.Rand
	if cfg.randomSeed == 0 {
		rng, _ = randutil.NewTestRand()
		t.L().Printf("Using random seed: %d", rng.Int63())
	} else {
		rng = rand.New(rand.NewSource(cfg.randomSeed))
		t.L().Printf("Using fixed seed: %d", cfg.randomSeed)
	}

	// Select and randomize schema operations
	selectedOps := selectRandomSchemaOps(allSchemaOps, cfg.numSchemaOps, rng)
	randomizedOps := addRandomDelays(selectedOps, cfg.minDelay, cfg.maxDelay, rng)

	// Build final plan
	plan := []modular.Operation{setupOp, tpccOp}
	plan = append(plan, randomizedOps...)

	// Add final validation
	validationOp := modular.NewOperation("validate schema", operations.ValidateSchema("tpcc")).Build("validate schema")
	plan = append(plan, validationOp)

	t.Status(fmt.Sprintf("running modular schema change test: %d warehouses, %d schema ops, %v duration",
		cfg.warehouses, cfg.numSchemaOps, cfg.duration))

	// Execute the plan
	if err := modular.RunPlan(ctx, t.L(), c, plan); err != nil {
		t.Fatal(err)
	}
}

// createSchemaChangeOperations defines all available schema change operations
func createSchemaChangeOperations(database string) []modular.Operation {
	return []modular.Operation{
		// Index operations
		modular.NewOperation("add index to order_line",
			addIndexToOrderLine(database)).Build("add index to order_line"),
		modular.NewOperation("add index to customer",
			addIndexToCustomer(database)).Build("add index to customer"),
		modular.NewOperation("add index to warehouse",
			addIndexToWarehouse(database)).Build("add index to warehouse"),

		// Column operations
		modular.NewOperation("add column to customer",
			operations.AddColumn(database, "customer", "c_notes", "TEXT")).Build("add column to customer"),
		modular.NewOperation("add column to warehouse",
			operations.AddColumn(database, "warehouse", "w_notes", "TEXT")).Build("add column to warehouse"),
		modular.NewOperation("add column to order_line",
			operations.AddColumn(database, "order_line", "ol_notes", "TEXT")).Build("add column to order_line"),

		// Table operations
		modular.NewOperation("create audit table",
			operations.CreateTable(database, "audit_log", "id SERIAL PRIMARY KEY, table_name TEXT, operation TEXT, timestamp TIMESTAMP DEFAULT NOW()")).Build("create audit table"),
		modular.NewOperation("create temp table",
			operations.CreateTable(database, "temp_data", "id INT PRIMARY KEY, data TEXT")).Build("create temp table"),

		// Random operations
		modular.NewOperation("random schema change",
			operations.RandomSchemaChange(database)).Build("random schema change"),
		operations.AddRandomIndex(),
	}
}

// Helper functions for specific schema changes
func addIndexToOrderLine(database string) func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
	return func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		_, err := h.CreateIndex("order_line_delivery_idx", database, "order_line",
			[]string{"ol_w_id", "ol_d_id", "ol_delivery_d"})
		if err != nil {
			return fmt.Errorf("failed to create order_line index: %w", err)
		}
		return nil
	}
}

func addIndexToCustomer(database string) func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
	return func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		_, err := h.CreateIndex("customer_name_idx", database, "customer",
			[]string{"c_last", "c_first"})
		if err != nil {
			return fmt.Errorf("failed to create customer index: %w", err)
		}
		return nil
	}
}

func addIndexToWarehouse(database string) func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
	return func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		_, err := h.CreateIndex("warehouse_name_idx", database, "warehouse",
			[]string{"w_name"})
		if err != nil {
			return fmt.Errorf("failed to create warehouse index: %w", err)
		}
		return nil
	}
}

// selectRandomSchemaOps picks a random subset of schema operations
func selectRandomSchemaOps(allOps []modular.Operation, numOps int, rng *rand.Rand) []modular.Operation {
	if numOps >= len(allOps) {
		// If we want more ops than available, return all ops shuffled
		selected := make([]modular.Operation, len(allOps))
		copy(selected, allOps)
		rng.Shuffle(len(selected), func(i, j int) {
			selected[i], selected[j] = selected[j], selected[i]
		})
		return selected
	}

	// Randomly select numOps operations
	indices := rng.Perm(len(allOps))[:numOps]
	selected := make([]modular.Operation, numOps)
	for i, idx := range indices {
		selected[i] = allOps[idx]
	}

	// Shuffle the selected operations
	rng.Shuffle(len(selected), func(i, j int) {
		selected[i], selected[j] = selected[j], selected[i]
	})

	return selected
}

// addRandomDelays wraps each operation with a random delay
func addRandomDelays(ops []modular.Operation, minDelay, maxDelay time.Duration, rng *rand.Rand) []modular.Operation {
	delayedOps := make([]modular.Operation, len(ops))

	for i, op := range ops {
		delay := minDelay + time.Duration(rng.Int63n(int64(maxDelay-minDelay)))
		// Capture op in closure
		capturedOp := op
		delayedOps[i] = modular.NewOperation(
			fmt.Sprintf("delayed %s (wait %v)", capturedOp.Name(), delay),
			operations.DelayedSchemaChange(delay, func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
				// Execute all steps in the operation's chain
				for _, stepGroup := range capturedOp.Chain() {
					for _, step := range stepGroup {
						if err := step.Run(ctx, l, h); err != nil {
							return err
						}
					}
				}
				return nil
			}),
		).Build(fmt.Sprintf("delayed %s", capturedOp.Name()))
	}

	return delayedOps
}
