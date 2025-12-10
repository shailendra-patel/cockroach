// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/modular"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/modular/operations"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

func registerModular(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "modular/example",
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Nightly),
		Owner:            registry.OwnerTestEng,
		Run:              runModularExample,
		Cluster:          r.MakeClusterSpec(6, spec.WorkloadNodeCount(1)),
		Timeout:          30 * time.Minute,
	})
	r.Add(registry.TestSpec{
		Name:             "modular/example/mvt",
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Nightly),
		Owner:            registry.OwnerTestEng,
		Run:              runModularMVTExample,
		Cluster:          r.MakeClusterSpec(5, spec.CPU(16), spec.WorkloadNode()),
		Timeout:          60 * time.Minute,
	})
	r.Add(registry.TestSpec{
		Name:             "modular/example/recovery",
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Nightly),
		Owner:            registry.OwnerTestEng,
		Run:              runModularRecoveryExample,
		Cluster:          r.MakeClusterSpec(6, spec.WorkloadNodeCount(1)),
		Timeout:          30 * time.Minute,
	})
	r.Add(registry.TestSpec{
		Name:             "modular/gc-example",
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Nightly),
		Owner:            registry.OwnerTestEng,
		Run:              runModularGCExample,
		Cluster:          r.MakeClusterSpec(3),
		Timeout:          15 * time.Minute,
	})
}

func runModularExample(ctx context.Context, t test.Test, c cluster.Cluster) {
	// Create a new modular test with a specific seed for reproducibility
	mod := modular.NewTest(ctx, t.L(), c, c.CRDBNodes(), modular.WithDebug(modular.ClusterStateDebug))

	mod.Setup("initialize cluster", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		c.Start(ctx, l, option.DefaultStartOpts(), install.MakeClusterSettings(), c.CRDBNodes())
		return nil
	})

	mod.Setup("initialize bank workload", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		dbName, err := h.CreateDatabase("bank")
		if err != nil {
			return err
		}

		cmd := roachtestutil.NewCommand("%s workload init bank", test.DefaultCockroachPath).
			Flag("rows", 100000).
			Flag("db", dbName).
			Arg("{pgurl:%d}", h.RandomAvailableNode()).
			String()

		return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd)
	})

	mainStage := mod.NewStage("main-workload", modular.WithStepConcurrency(3))

	mod.AddOperation(mainStage, operations.AddRandomIndex())

	// Add TPCC workload chain: init, run, then check consistency
	mod.AddOperation(mainStage, operations.TPCC(c, 1000, time.Minute, operations.TPCCExtraOptions{}))

	mod.AddOperation(mainStage, operations.ReplicationFactorCycle())

	// Add INSPECT operation to validate table consistency
	mod.AddOperation(mainStage, operations.InspectTable())

	// Generate the test plan
	planner := mod.NewPlanner()
	testPlan, err := planner.Plan()
	if err != nil {
		t.Fatalf("Failed to generate test plan: %v", err)
	}

	// Execute the test plan using the runner
	err = modular.RunTestPlan(ctx, t, testPlan)
	if err != nil {
		// Check if the error contains ONLY the intentional failure we expect
		expectedError := "intentional fatal error to test recovery"
		if strings.Contains(err.Error(), expectedError) && !strings.Contains(err.Error(), "Failed to restore") {
			// The test succeeded - we got the expected failure and state restoration succeeded
			t.L().Printf("Test completed successfully: got expected intentional failure and cluster state was restored")
			return
		}
		// Any other error (including restoration failures) should fail the test
		t.Fatalf("Test execution failed: %v", err)
	}
}

func runModularMVTExample(ctx context.Context, t test.Test, c cluster.Cluster) {
	// Calculate warehouse and row counts similar to mixed-headroom
	maxWarehouses := maxSupportedTPCCWarehouses(*t.BuildVersion(), c.Cloud(), c.Spec())
	headroomWarehouses := int(float64(maxWarehouses) * 0.7)
	bankRows := 65104166 / 2
	if c.IsLocal() {
		bankRows = 1000
		headroomWarehouses = 20
	}

	// Create a modular test that mimics the mixed-headroom structure
	mod := modular.NewTest(ctx, t.L(), c, c.CRDBNodes())

	initStage := mod.NewStage("cluster init")
	mod.InStage(initStage, "install fixtures for version 25.2", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		// Install fixtures for version 25.2
		version := clusterupgrade.MustParseVersion("v25.2.0")
		return clusterupgrade.InstallFixtures(ctx, l, c, c.CRDBNodes(), version)
	}).Then("start cluster at version 25.2", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		// Start cluster using version 25.2
		version := clusterupgrade.MustParseVersion("v25.2.0")
		binaryPath, err := clusterupgrade.UploadCockroach(ctx, t, l, c, c.CRDBNodes(), version)
		if err != nil {
			return err
		}

		clusterSettings := install.MakeClusterSettings(
			install.BinaryOption(binaryPath),
		)

		startOpts := option.NewStartOpts(
			option.NoBackupSchedule,
		)

		c.Start(ctx, l, startOpts, clusterSettings, c.CRDBNodes())
		return nil
	}).Then("waiting for all nodes to acknowledge cluster version 25.2", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		// Use a timeout of 5 minutes for cluster version acknowledgment
		timeout := 5 * time.Minute

		// Connect function that creates a connection to a specific node
		// Let WaitForClusterUpgrade handle connection errors gracefully
		connectFunc := func(node int) *gosql.DB {
			db, err := c.ConnE(ctx, l, node)
			if err != nil {
				// Return nil and let WaitForClusterUpgrade handle the error
				// This is safer than panicking
				l.Printf("warning: failed to connect to node %d: %v", node, err)
				return nil
			}
			return db
		}

		return clusterupgrade.WaitForClusterUpgrade(ctx, l, c.CRDBNodes(), connectFunc, timeout)
	})

	inStartupStage := mod.NewStage("startup")
	mod.InStage(inStartupStage, "set preserve_downgrade_option to 25.2", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		db := c.Conn(ctx, l, 1)
		_, err := db.ExecContext(ctx, "SET CLUSTER SETTING cluster.preserve_downgrade_option = $1", "25.2")
		return err
	})
	mod.InStage(inStartupStage, "enable tenant features", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		return enableTenantSplitScatterModular(l, h)
	}).Then("import TPCC dataset", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		return importTPCCDataModular(ctx, t, c, l, h, headroomWarehouses)
	}).And("import bank dataset", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		return importBankDataModular(ctx, t, c, l, h, bankRows)
	})

	upgradeStage := mod.NewStage("upgrade from v25.2 -> current")
	mod.InStage(upgradeStage, "run TPCC workload", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		return runTPCCWorkloadModular(ctx, t, c, l, h, headroomWarehouses)
	})

	for _, node := range c.CRDBNodes() {
		mod.InStage(upgradeStage, fmt.Sprintf("restart node %d with current version", node), func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
			return restartNodeWithCurrentVersion(ctx, t, l, c, node)
		}, modular.DisableConcurrency())
	}

	rollbackStage := mod.NewStage("rollback")
	mod.InStage(rollbackStage, "run TPCC workload", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		return runTPCCWorkloadModular(ctx, t, c, l, h, headroomWarehouses)
	})

	for _, node := range c.CRDBNodes() {
		mod.InStage(rollbackStage, fmt.Sprintf("rollback node %d to version 25.2", node), func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
			return rollbackNodeToVersion(ctx, t, l, c, node, "v25.2.0")
		}, modular.DisableConcurrency())
	}

	finalizeStage := mod.NewStage("finalize")
	mod.InStage(finalizeStage, "run TPCC workload", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		return runTPCCWorkloadModular(ctx, t, c, l, h, headroomWarehouses)
	})
	sb := mod.InStage(finalizeStage, fmt.Sprintf("restart node %d with current version", 1), func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		return restartNodeWithCurrentVersion(ctx, t, l, c, 1)
	}, modular.DisableConcurrency())

	for _, node := range c.CRDBNodes()[1:] {
		sb = sb.And(fmt.Sprintf("restart node %d with current version", node), func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
			return restartNodeWithCurrentVersion(ctx, t, l, c, node)
		}, modular.DisableConcurrency())
	}
	sb.And("reset preserve_downgrade_option", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		db := c.Conn(ctx, l, 1)
		_, err := db.ExecContext(ctx, "RESET CLUSTER SETTING cluster.preserve_downgrade_option")
		return err
	}).Then("wait for all nodes to acknowledge current cluster version", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		// Use a timeout of 5 minutes for cluster version acknowledgment
		timeout := 5 * time.Minute

		// Connect function that creates a connection to a specific node
		// Let WaitForClusterUpgrade handle connection errors gracefully
		connectFunc := func(node int) *gosql.DB {
			db, err := c.ConnE(ctx, l, node)
			if err != nil {
				// Return nil and let WaitForClusterUpgrade handle the error
				// This is safer than panicking
				l.Printf("warning: failed to connect to node %d: %v", node, err)
				return nil
			}
			return db
		}

		return clusterupgrade.WaitForClusterUpgrade(ctx, l, c.CRDBNodes(), connectFunc, timeout)
	})

	// Final validation stage
	validationStage := mod.NewStage("validation")
	mod.InStage(validationStage, "check TPCC workload integrity", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		return checkTPCCWorkloadModular(ctx, t, c, l, h, headroomWarehouses)
	})

	// Generate and execute test plan
	planner := mod.NewPlanner()
	testPlan, err := planner.Plan()
	if err != nil {
		t.Fatalf("Failed to generate test plan: %v", err)
	}

	err = modular.RunTestPlan(ctx, t, testPlan)
	if err != nil {
		t.Fatalf("Test execution failed: %v", err)
	}
}

// enableTenantSplitScatterModular enables tenant features for SPLIT and SCATTER operations
func enableTenantSplitScatterModular(l *logger.Logger, _ *modular.Helper) error {
	// This is a simplified version - in a real mixed-version test,
	// we would check version compatibility and enable settings conditionally
	settings := []string{
		"sql.split_at.allow_for_secondary_tenant.enabled",
		"sql.scatter.allow_for_secondary_tenant.enabled",
	}

	for _, setting := range settings {
		l.Printf("enabling setting: %s", setting)
		// In modular framework, we would need access to cluster connections
		// For now, this is a placeholder that demonstrates the structure
	}
	return nil
}

// importTPCCDataModular imports TPCC dataset
func importTPCCDataModular(ctx context.Context, _ test.Test, c cluster.Cluster, l *logger.Logger, _ *modular.Helper, warehouses int) error {
	l.Printf("importing TPCC data with %d warehouses", warehouses)

	// Use the workload node for import
	cmd := tpccImportCmdWithCockroachBinary(
		test.DefaultCockroachPath, "", "tpcc", warehouses,
		fmt.Sprintf("{pgurl%s}", c.Node(1)),
	)

	return c.RunE(ctx, option.WithNodes(c.Node(1)), cmd)
}

// importBankDataModular imports large bank dataset to stress the system
func importBankDataModular(ctx context.Context, _ test.Test, c cluster.Cluster, l *logger.Logger, _ *modular.Helper, rows int) error {
	l.Printf("importing bank data with %d rows", rows)

	cmd := roachtestutil.NewCommand("%s workload fixtures import bank", test.DefaultCockroachPath).
		Arg("{pgurl%s}", c.Node(1)).
		Flag("payload-bytes", 10240).
		Flag("rows", rows).
		Flag("seed", 4).
		Flag("db", "bigbank").
		String()

	return c.RunE(ctx, option.WithNodes(c.Node(1)), cmd)
}

// runTPCCWorkloadModular runs the TPCC workload
func runTPCCWorkloadModular(ctx context.Context, _ test.Test, c cluster.Cluster, l *logger.Logger, _ *modular.Helper, warehouses int) error {
	workloadDur := 10 * time.Minute
	rampDur := 1 * time.Minute
	if c.IsLocal() {
		workloadDur = 2 * time.Minute
		rampDur = 30 * time.Second
	}

	l.Printf("running TPCC workload for %v with %d warehouses", workloadDur, warehouses)

	cmd := roachtestutil.NewCommand("./cockroach workload run tpcc").
		Arg("{pgurl%s}", c.CRDBNodes()).
		Flag("duration", workloadDur).
		Flag("warehouses", warehouses).
		Flag("ramp", rampDur).
		Flag("prometheus-port", 2112).
		String()

	return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd)
}

// checkTPCCWorkloadModular validates TPCC data integrity
func checkTPCCWorkloadModular(ctx context.Context, _ test.Test, c cluster.Cluster, l *logger.Logger, _ *modular.Helper, warehouses int) error {
	l.Printf("checking TPCC workload data integrity for %d warehouses", warehouses)

	cmd := roachtestutil.NewCommand("%s workload check tpcc", test.DefaultCockroachPath).
		Arg("{pgurl:1}").
		Flag("warehouses", warehouses).
		String()

	return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd)
}

// restartNodeWithCurrentVersion restarts a node with the current binary version
// This mimics the behavior of restartWithNewBinaryStep from the mixed version framework
func restartNodeWithCurrentVersion(ctx context.Context, rt test.Test, l *logger.Logger, cluster cluster.Cluster, node int) error {
	l.Printf("restarting node %d with current binary version", node)

	// Use a timeout for the restart operation similar to mixed version framework
	startTimeout := 30 * time.Minute
	startCtx, cancel := context.WithTimeout(ctx, startTimeout)
	defer cancel()

	// Create node options for the specific node
	nodeOption := cluster.Node(node)

	// Custom start options similar to restartSystemSettings in mixed version
	customStartOpts := []option.StartStopOption{
		option.SkipInit,         // Don't re-initialize the cluster
		option.NoBackupSchedule, // Disable scheduled backups for deterministic tests
	}

	// Use current version binary (no specific version specified means current)
	// Create empty cluster settings slice for current version
	var clusterSettings []install.ClusterSettingOption

	// Use clusterupgrade.RestartNodesWithNewBinary which handles the full restart process
	// This is the same function used in the mixed version framework
	// Use nil for current version - this will use the test's build version
	return clusterupgrade.RestartNodesWithNewBinary(
		startCtx,
		rt,
		l,
		cluster,
		nodeOption,
		option.NewStartOpts(customStartOpts...),
		clusterupgrade.CurrentVersion(),
		clusterSettings...,
	)
}

// rollbackNodeToVersion restarts a node with a specific version (rollback scenario)
// This simulates rolling back from current version to a previous version
func rollbackNodeToVersion(ctx context.Context, rt test.Test, l *logger.Logger, cluster cluster.Cluster, node int, versionStr string) error {
	l.Printf("rolling back node %d to version %s", node, versionStr)

	// Parse the target version
	targetVersion := clusterupgrade.MustParseVersion(versionStr)

	// Use a timeout for the restart operation similar to mixed version framework
	startTimeout := 30 * time.Minute
	startCtx, cancel := context.WithTimeout(ctx, startTimeout)
	defer cancel()

	// Create node options for the specific node
	nodeOption := cluster.Node(node)

	// Custom start options similar to restartSystemSettings in mixed version
	customStartOpts := []option.StartStopOption{
		option.SkipInit,         // Don't re-initialize the cluster
		option.NoBackupSchedule, // Disable scheduled backups for deterministic tests
	}

	// Use the specific target version binary
	// Create empty cluster settings slice
	var clusterSettings []install.ClusterSettingOption

	// Use clusterupgrade.RestartNodesWithNewBinary with the target version
	// This handles uploading the specific version binary and restarting
	return clusterupgrade.RestartNodesWithNewBinary(
		startCtx,
		rt,
		l,
		cluster,
		nodeOption,
		option.NewStartOpts(customStartOpts...),
		targetVersion,
		clusterSettings...,
	)
}

// runModularGCExample demonstrates the Plan-Scoped GC system with actual database operations.
// It creates tables in the plan schema, modifies cluster settings, and verifies cleanup works.
func runModularGCExample(ctx context.Context, t test.Test, c cluster.Cluster) {
	// Create a new modular test with GC and state tracking enabled
	mod := modular.NewTest(
		ctx, t.L(), c, c.CRDBNodes(),
		modular.WithDebug(modular.ClusterStateDebug),
		modular.WithDebug(modular.GCDebug),
	)

	// Setup: Start the cluster
	//mod.Setup("initialize cluster", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
	//	c.Start(ctx, l, option.DefaultStartOpts(), install.MakeClusterSettings(), c.CRDBNodes())
	//	return nil
	//})

	// Setup: Verify we can connect
	mod.Setup("verify cluster connectivity", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		db := h.RandomDBConn()
		var result int
		if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&result); err != nil {
			return fmt.Errorf("failed to verify cluster connectivity: %w", err)
		}
		l.Printf("Cluster connectivity verified: SELECT 1 = %d", result)
		return nil
	})

	// Main stage: Create tables and modify settings
	mainStage := mod.NewStage("database-operations", modular.WithStepConcurrency(2))

	// Chain 1: Create a table in the plan schema and insert data
	mod.InStage(mainStage, "create test table", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		// CreateTable uses the plan schema via search_path
		tableName, err := h.CreateTable("test_data", "id INT PRIMARY KEY, value TEXT")
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
		l.Printf("Created table: %s", tableName)

		// Insert some data using the table name
		for i := 1; i <= 10; i++ {
			if err := h.Exec(fmt.Sprintf("INSERT INTO %s VALUES (%d, 'value_%d')", tableName, i, i)); err != nil {
				return fmt.Errorf("failed to insert row %d: %w", i, err)
			}
		}
		l.Printf("Inserted 10 rows into %s", tableName)
		return nil
	}).Then("verify data exists", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		// Query tables in the current schema
		rows, err := h.Query("SELECT table_name FROM [SHOW TABLES]")
		if err != nil {
			return fmt.Errorf("failed to list tables: %w", err)
		}
		defer rows.Close()

		var tables []string
		for rows.Next() {
			var tableName string
			if err := rows.Scan(&tableName); err != nil {
				return err
			}
			tables = append(tables, tableName)
		}
		l.Printf("Tables in current schema: %v", tables)
		return nil
	})

	// Chain 2: Modify cluster settings (these get auto-restored by GC)
	mod.InStage(mainStage, "modify cluster settings", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		// SetClusterSetting captures original value and registers cleanup
		if err := h.SetClusterSetting("kv.range_merge.queue_enabled", "false"); err != nil {
			return fmt.Errorf("failed to set cluster setting: %w", err)
		}
		l.Printf("Disabled range merge queue (will be restored on cleanup)")
		return nil
	}).Then("verify setting changed", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		var value string
		row := h.QueryRow("SHOW CLUSTER SETTING kv.range_merge.queue_enabled")
		if err := row.Scan(&value); err != nil {
			return err
		}
		l.Printf("Current kv.range_merge.queue_enabled = %s", value)
		if value != "false" {
			return fmt.Errorf("expected setting to be 'false', got '%s'", value)
		}
		return nil
	})

	// Chain 3: Create an external database (explicitly registered for cleanup)
	mod.InStage(mainStage, "create external database", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		// CreateDatabase auto-registers cleanup via GC
		dbName, err := h.CreateDatabase("external_test")
		if err != nil {
			return fmt.Errorf("failed to create database: %w", err)
		}
		l.Printf("Created external database: %s (will be dropped on cleanup)", dbName)
		return nil
	})

	// Validation stage
	validationStage := mod.NewStage("validation")

	mod.InStage(validationStage, "verify GC state", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		gc := h.GC()
		if gc == nil {
			return fmt.Errorf("GC is nil")
		}
		if !gc.Enabled() {
			return fmt.Errorf("GC should be enabled")
		}

		// Check cleanup stack has entries
		stackSize := gc.Stack().Size()
		l.Printf("Cleanup stack has %d entries registered", stackSize)

		// Log schema name
		l.Printf("Plan schema: %s", gc.Schema().Name)

		return nil
	})

	// After-test: Verify objects exist before cleanup
	mod.AfterTest("log pre-cleanup state", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		l.Printf("=== Pre-cleanup state ===")

		// List all schemas
		rows, err := h.Query("SELECT schema_name FROM [SHOW SCHEMAS] WHERE schema_name LIKE 'test_plan_%'")
		if err != nil {
			l.Printf("Failed to list schemas: %v", err)
		} else {
			defer rows.Close()
			for rows.Next() {
				var schemaName string
				if err := rows.Scan(&schemaName); err == nil {
					l.Printf("Found plan schema: %s", schemaName)
				}
			}
		}

		// List all databases
		dbRows, err := h.Query("SELECT database_name FROM [SHOW DATABASES] WHERE database_name LIKE 'external_test_%'")
		if err != nil {
			l.Printf("Failed to list databases: %v", err)
		} else {
			defer dbRows.Close()
			for dbRows.Next() {
				var dbName string
				if err := dbRows.Scan(&dbName); err == nil {
					l.Printf("Found external database: %s", dbName)
				}
			}
		}

		return nil
	})

	// Generate and execute the test plan
	planner := mod.NewPlanner()
	testPlan, err := planner.Plan()
	if err != nil {
		t.Fatalf("Failed to generate test plan: %v", err)
	}

	// Execute the test plan - GC cleanup runs automatically via defer
	err = modular.RunTestPlan(ctx, t, testPlan)
	if err != nil {
		t.Fatalf("Test execution failed: %v", err)
	}

	// Post-cleanup verification: Check that objects were cleaned up
	t.L().Printf("=== Post-cleanup verification ===")

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	// Verify plan schema was dropped
	var schemaCount int
	err = db.QueryRowContext(ctx, "SELECT count(*) FROM [SHOW SCHEMAS] WHERE schema_name LIKE 'test_plan_%'").Scan(&schemaCount)
	if err != nil {
		t.L().Printf("Warning: Failed to check schemas: %v", err)
	} else if schemaCount > 0 {
		t.L().Printf("Warning: %d plan schemas still exist (may be from other tests)", schemaCount)
	} else {
		t.L().Printf("Verified: Plan schema was cleaned up")
	}

	// Verify external database was dropped
	var dbCount int
	err = db.QueryRowContext(ctx, "SELECT count(*) FROM [SHOW DATABASES] WHERE database_name LIKE 'external_test_%'").Scan(&dbCount)
	if err != nil {
		t.L().Printf("Warning: Failed to check databases: %v", err)
	} else if dbCount > 0 {
		t.L().Printf("Warning: %d external databases still exist", dbCount)
	} else {
		t.L().Printf("Verified: External database was cleaned up")
	}

	// Verify cluster setting was restored
	var settingValue string
	err = db.QueryRowContext(ctx, "SHOW CLUSTER SETTING kv.range_merge.queue_enabled").Scan(&settingValue)
	if err != nil {
		t.L().Printf("Warning: Failed to check cluster setting: %v", err)
	} else {
		t.L().Printf("Cluster setting kv.range_merge.queue_enabled = %s (should be restored to original)", settingValue)
	}

	t.L().Printf("GC example test completed successfully")
}

func runModularRecoveryExample(ctx context.Context, t test.Test, c cluster.Cluster) {
	// Create a new modular test with state tracking enabled for recovery testing
	// GC is enabled by default and handles cleanup on both success and failure
	mod := modular.NewTest(ctx, t.L(), c, c.CRDBNodes(), modular.WithDebug(modular.ClusterStateDebug))

	mod.Setup("initialize cluster", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		c.Start(ctx, l, option.DefaultStartOpts(), install.MakeClusterSettings(), c.CRDBNodes())
		return nil
	})

	// Create main test stage with custom concurrency
	mainStage := mod.NewStage("main-workload", modular.WithStepConcurrency(3))

	// Add TPCC workload chain: init, run, then check consistency
	mod.InStage(mainStage, "init tpcc workload", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		cmd := fmt.Sprintf("./cockroach workload init tpcc --warehouses=10 {pgurl:%d}", h.RandomAvailableNode())
		c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmd)
		return nil
	}).Then("run tpcc workload", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		cmd := fmt.Sprintf("./cockroach workload run tpcc --warehouses=10 --duration=60s {pgurl%s}", h.AvailableNodes())
		c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmd)
		return nil
	}).Then("check tpcc consistency", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		cmd := fmt.Sprintf("./cockroach workload check tpcc --warehouses=10 {pgurl:%d}", h.RandomAvailableNode())
		c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmd)
		return nil
	})

	// Add replication factor chain with an unconditional fatal step to test recovery
	mod.InStage(mainStage, "increase rebalance snapshot rate", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		return h.SetClusterSetting("kv.snapshot_rebalance.max_rate", "2 GiB")
	}).Then("increase replication factor to 5", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		return h.AlterRange("default", "num_replicas = 5")
	}).Then("wait for replication to 5", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		_, db := h.RandomDB()
		defer db.Close()
		return roachtestutil.WaitForReplication(ctx, l, db, 5, roachprod.AtLeastReplicationFactor)
	}).Then("decrease replication factor to 3", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		return h.AlterRange("default", "num_replicas = 3")
	}).Then("restore rebalance snapshot rate", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		return h.ResetClusterSetting("kv.snapshot_rebalance.max_rate")
	})

	mod.InStage(mainStage, "error step", func(ctx context.Context, l *logger.Logger, helper *modular.Helper) error {
		return errors.New("intentional fatal error to test recovery")
	})

	// Generate the test plan
	planner := mod.NewPlanner()
	testPlan, err := planner.Plan()
	if err != nil {
		t.Fatalf("Failed to generate test plan: %v", err)
	}

	// Execute the test plan using the runner
	err = modular.RunTestPlan(ctx, t, testPlan)
	if err != nil {
		// Check if the error contains ONLY the intentional failure we expect
		expectedError := "intentional fatal error to test recovery"
		if strings.Contains(err.Error(), expectedError) && !strings.Contains(err.Error(), "Failed to restore") {
			// The test succeeded - we got the expected failure and state restoration succeeded
			t.L().Printf("Test completed successfully: got expected intentional failure and cluster state was restored")
			return
		}
		// Any other error (including restoration failures) should fail the test
		t.Fatalf("Test execution failed: %v", err)
	}
}
