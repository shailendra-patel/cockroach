package modular

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

func joinArgs(args ...string) string {
	if len(args) == 0 {
		return ""
	}
	return strings.Join(args, " ")
}

const (
	logPrefix = "modular-test"
)

// ddlPrefixes are SQL statement prefixes that must use dedicated Helper methods
// to ensure proper schema-based garbage collection.
var ddlPrefixes = []string{
	"CREATE TABLE",
	"CREATE INDEX",
	"CREATE DATABASE",
	"CREATE SCHEMA",
	"CREATE USER",
	"ALTER TABLE",
	"ALTER INDEX",
	"ALTER DATABASE",
	"ALTER SCHEMA",
	"ALTER USER",
	"ALTER RANGE",
	"SET CLUSTER SETTING",
	"RESET CLUSTER SETTING",
	"GRANT",
	"REVOKE",
	"TRUNCATE",
}

// ddlMethodGuide maps DDL prefixes to recommended Helper methods.
var ddlMethodGuide = map[string]string{
	"CREATE TABLE":          "CreateTable()",
	"CREATE INDEX":          "CreateIndex()",
	"CREATE DATABASE":       "CreateDatabase()",
	"CREATE SCHEMA":         "CreateSchema()",
	"CREATE USER":           "CreateUser() or CreateUserPassword()",
	"DROP DATABASE":         "DropDatabase()",
	"SET CLUSTER SETTING":   "SetClusterSetting()",
	"RESET CLUSTER SETTING": "ResetClusterSetting()",
	"ALTER RANGE":           "AlterRange() or AlterAllRanges()",
	"ALTER TABLE":           "AlterTable() or AddColumn()/DropColumn()",
	"TRUNCATE":              "TruncateTable()",
}

// validateNotDDL checks if a query is a DDL statement and panics if so.
// This enforces that DDL operations go through tracked Helper methods.
func validateNotDDL(query string) {
	normalized := strings.ToUpper(strings.TrimSpace(query))

	for _, prefix := range ddlPrefixes {
		if strings.HasPrefix(normalized, prefix) {
			suggestion := ddlMethodGuide[prefix]
			if suggestion == "" {
				suggestion = "a dedicated Helper method"
			}
			panic(fmt.Sprintf(
				"DDL statement detected in Exec(): %q\n"+
					"DDL must go through tracked Helper methods for proper cleanup.\n"+
					"Use %s instead.",
				query, suggestion,
			))
		}
	}
}

// Helper provides utilities for modular test steps.
type Helper struct {
	defaultService *Service
	rng            *rand.Rand
	// taskCount keeps track of the number of tasks started with `helper.Go()`.
	// The counter is used to generate unique log file names.
	taskCount  int64
	cluster    cluster.Cluster
	logger     *logger.Logger
	background task.Manager
	ctx        context.Context
	//stateTracker *ClusterStateTracker
	// gc is the garbage collector for plan-scoped cleanup.
	// It manages the plan schema and cleanup of global objects.
	gc *GarbageCollector
}

func (h *Helper) AvailableNodes() option.NodeListOption {
	return h.defaultService.AvailableNodes()
}

func (h *Helper) RandomAvailableNode() int {
	nodes := h.AvailableNodes()
	return nodes.SeededRandNode(h.rng)[0]
}

// RandomDB is like RandomDBConn, but also returns the node ID.
func (h *Helper) RandomDB() (int, *gosql.DB) {
	return h.defaultService.RandomDB(h.rng)
}

// Query performs `db.QueryContext` on a randomly picked database node. The
// query and the node picked are logged in the logs of the step that calls this
// function.
func (h *Helper) Query(query string, args ...interface{}) (*gosql.Rows, error) {
	return h.defaultService.Query(h.rng, query, args...)
}

// QueryRow performs `db.QueryRowContext` on a randomly picked
// database node. The query and the node picked are logged in the logs
// of the step that calls this function.
func (h *Helper) QueryRow(query string, args ...interface{}) *gosql.Row {
	return h.defaultService.QueryRow(h.rng, query, args...)
}

// Exec performs `db.ExecContext` on a randomly picked database node.
// The query and the node picked are logged in the logs of the step
// that calls this function.
//
// IMPORTANT: This method is for DML statements only (INSERT, UPDATE, DELETE).
// DDL statements (CREATE, DROP, ALTER, etc.) must use dedicated Helper methods
// like CreateTable(), CreateDatabase(), SetClusterSetting() to ensure proper
// cleanup tracking. Attempting to run DDL via Exec will panic.
func (h *Helper) Exec(query string, args ...interface{}) error {
	validateNotDDL(query)
	return h.defaultService.Exec(h.rng, query, args...)
}

// ExecWithGateway is like Exec, but allows the caller to specify the
// set of nodes that should be used as gateway.
//
// IMPORTANT: This method is for DML statements only. See Exec() for details.
func (h *Helper) ExecWithGateway(
	nodes option.NodeListOption, query string, args ...interface{},
) error {
	validateNotDDL(query)
	return h.defaultService.ExecWithGateway(h.rng, nodes, query, args...)
}

// execInternal executes a query without DDL validation.
// This is used by Helper's DDL methods which are already tracked.
func (h *Helper) execInternal(query string, args ...interface{}) error {
	return h.defaultService.Exec(h.rng, query, args...)
}

// execInternalWithGateway is like execInternal but with specific gateway nodes.
func (h *Helper) execInternalWithGateway(
	nodes option.NodeListOption, query string, args ...interface{},
) error {
	return h.defaultService.ExecWithGateway(h.rng, nodes, query, args...)
}

// CreateTable creates a table with the specified schema.
// The table is automatically registered for cleanup when GC runs.
func (h *Helper) CreateTable(namePrefix, schema string) (string, error) {
	tableName := generateRandomName(namePrefix)
	query := fmt.Sprintf("CREATE TABLE %s (%s)", tableName, schema)
	if err := h.execInternal(query); err != nil {
		return "", err
	}

	h.RegisterCleanup(
		fmt.Sprintf("Drop table %s", tableName),
		fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", tableName),
	)

	return tableName, nil
}

// SetClusterSetting sets a cluster setting and registers restoration on cleanup.
// Cluster settings are global and cannot be scoped to a schema, so we capture
// the original value and register a cleanup statement to restore it.
func (h *Helper) SetClusterSetting(settingName, newValue string) error {
	// Capture the current value for restoration during cleanup
	if h.gc.Enabled() {
		var currentValue string
		row := h.QueryRow(fmt.Sprintf("SHOW CLUSTER SETTING %s", settingName))
		if err := row.Scan(&currentValue); err != nil {
			return fmt.Errorf("failed to read current value of %s: %w", settingName, err)
		}

		// Register cleanup to restore original value (LIFO order)
		h.RegisterCleanup(
			fmt.Sprintf("Restore cluster setting %s to '%s'", settingName, currentValue),
			fmt.Sprintf("SET CLUSTER SETTING %s = '%s'", settingName, currentValue),
		)
	}

	// Use parameterized query for the value but format the setting name
	query := fmt.Sprintf("SET CLUSTER SETTING %s = $1", settingName)
	return h.execInternal(query, newValue)
}

// ResetClusterSetting resets a cluster setting to its default value.
// If GC is enabled, it captures the current value before reset to allow restoration.
func (h *Helper) ResetClusterSetting(settingName string) error {
	// Capture the current value for restoration during cleanup
	if h.gc != nil && h.gc.Enabled() {
		var currentValue string
		row := h.QueryRow(fmt.Sprintf("SHOW CLUSTER SETTING %s", settingName))
		if err := row.Scan(&currentValue); err != nil {
			return fmt.Errorf("failed to read current value of %s: %w", settingName, err)
		}

		// Register cleanup to restore original value (LIFO order)
		h.RegisterCleanup(
			fmt.Sprintf("Restore cluster setting %s to '%s'", settingName, currentValue),
			fmt.Sprintf("SET CLUSTER SETTING %s = '%s'", settingName, currentValue),
		)
	}
	return h.execInternal(fmt.Sprintf("RESET CLUSTER SETTING %s", settingName))
}

// CreateUser creates a user with automatic name generation.
// The user is automatically registered for cleanup when GC runs.
func (h *Helper) CreateUser(namePrefix string, args ...string) (string, error) {
	username := generateRandomName(namePrefix)
	query := fmt.Sprintf("CREATE USER %s %s", username, joinArgs(args...))
	if err := h.execInternal(query); err != nil {
		return "", err
	}

	h.RegisterCleanup(
		fmt.Sprintf("Drop user %s", username),
		fmt.Sprintf("DROP USER IF EXISTS %s", username),
	)

	return username, nil
}

func (h *Helper) CreateUserPassword(namePrefix, password string, args ...string) (string, error) {
	args = append([]string{fmt.Sprintf("WITH PASSWORD %s", password)}, args...)
	return h.CreateUser(namePrefix, args...)
}

// CreateDatabase creates a database with automatic name generation and tracking.
// Since databases are external to the plan schema, cleanup is registered.
func (h *Helper) CreateDatabase(namePrefix string, args ...string) (string, error) {
	dbName := generateRandomName(namePrefix)
	query := fmt.Sprintf("CREATE DATABASE %s %s", dbName, joinArgs(args...))
	if err := h.execInternal(strings.TrimSpace(query)); err != nil {
		return "", err
	}

	h.RegisterCleanup(
		fmt.Sprintf("Drop database %s", dbName),
		fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", dbName),
	)

	return dbName, nil
}

// RegisterCleanup allows registering a custom cleanup statement.
// Use this for objects created outside of standard Helper methods,
// such as databases created by external workloads (TPCC, YCSB, etc.).
func (h *Helper) RegisterCleanup(description, statement string) {
	if h.gc != nil && h.gc.Enabled() {
		h.gc.RegisterCleanup(description, statement)
	}
}

// GC returns the garbage collector for direct access if needed.
// Use with caution; prefer using Helper methods that auto-register cleanup.
func (h *Helper) GC() *GarbageCollector {
	return h.gc
}

// CreateSchema creates a schema with automatic name generation and tracking.
// The schema is automatically registered for cleanup when GC runs.
func (h *Helper) CreateSchema(namePrefix string, args ...string) (string, error) {
	schemaName := generateRandomName(namePrefix)
	query := fmt.Sprintf("CREATE SCHEMA %s %s", schemaName, joinArgs(args...))
	if err := h.execInternal(strings.TrimSpace(query)); err != nil {
		return "", err
	}

	h.RegisterCleanup(
		fmt.Sprintf("Drop schema %s", schemaName),
		fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaName),
	)

	return schemaName, nil
}

// CreateIndex creates an index with automatic name generation and tracking.
// The index is automatically registered for cleanup when GC runs.
func (h *Helper) CreateIndex(namePrefix, database, table string, columns []string, args ...string) (string, error) {
	indexName := generateRandomName(namePrefix)
	columnsStr := strings.Join(columns, ", ")
	tableRef := fmt.Sprintf("%s.%s", database, table)
	query := fmt.Sprintf("CREATE INDEX %s ON %s (%s) %s", indexName, tableRef, columnsStr, joinArgs(args...))
	if err := h.execInternal(strings.TrimSpace(query)); err != nil {
		return "", err
	}

	h.RegisterCleanup(
		fmt.Sprintf("Drop index %s on %s", indexName, tableRef),
		fmt.Sprintf("DROP INDEX IF EXISTS %s@%s CASCADE", tableRef, indexName),
	)

	return indexName, nil
}

// AddColumn adds a column to a table.
// The column is automatically registered for cleanup (DROP COLUMN) when GC runs.
func (h *Helper) AddColumn(database, table, columnName, columnType string, args ...string) error {
	tableRef := fmt.Sprintf("%s.%s", database, table)
	query := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s %s",
		tableRef, columnName, columnType, joinArgs(args...))
	if err := h.execInternal(strings.TrimSpace(query)); err != nil {
		return err
	}

	h.RegisterCleanup(
		fmt.Sprintf("Drop column %s from %s", columnName, tableRef),
		fmt.Sprintf("ALTER TABLE %s DROP COLUMN IF EXISTS %s CASCADE", tableRef, columnName),
	)

	return nil
}

// DropTable drops a table.
func (h *Helper) DropTable(database, table string) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s CASCADE", database, table)
	return h.execInternal(query)
}

// DropIndex drops an index.
func (h *Helper) DropIndex(database, table, indexName string) error {
	query := fmt.Sprintf("DROP INDEX IF EXISTS %s.%s@%s CASCADE", database, table, indexName)
	return h.execInternal(query)
}

// DropDatabase drops a database.
func (h *Helper) DropDatabase(database string) error {
	query := fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", database)
	return h.execInternal(query)
}

// DropColumn drops a column from a table.
func (h *Helper) DropColumn(database, table, columnName string) error {
	query := fmt.Sprintf("ALTER TABLE %s.%s DROP COLUMN %s CASCADE", database, table, columnName)
	return h.execInternal(query)
}

// TruncateTable truncates a table.
func (h *Helper) TruncateTable(database, table string) error {
	query := fmt.Sprintf("TRUNCATE TABLE %s.%s", database, table)
	return h.execInternal(query)
}

// Grant grants privileges and registers a cleanup to revoke them.
func (h *Helper) Grant(privilege, objectType, objectName, grantee string) error {
	query := fmt.Sprintf("GRANT %s ON %s %s TO %s", privilege, objectType, objectName, grantee)
	if err := h.execInternal(query); err != nil {
		return err
	}
	// Register cleanup to revoke the privilege
	h.RegisterCleanup(
		fmt.Sprintf("Revoke %s on %s %s from %s", privilege, objectType, objectName, grantee),
		fmt.Sprintf("REVOKE %s ON %s %s FROM %s", privilege, objectType, objectName, grantee),
	)
	return nil
}

// Revoke revokes privileges.
func (h *Helper) Revoke(privilege, objectType, objectName, grantee string) error {
	query := fmt.Sprintf("REVOKE %s ON %s %s FROM %s", privilege, objectType, objectName, grantee)
	return h.execInternal(query)
}

// AlterRange alters a range's zone configuration.
// If GC is enabled, it captures the original zone config before modification.
func (h *Helper) AlterRange(rangeName, zoneConfig string) error {
	// Capture the original zone config for restoration during cleanup
	if h.gc != nil && h.gc.Enabled() {
		var currentConfig string
		row := h.QueryRow(fmt.Sprintf("SHOW ZONE CONFIGURATION FOR RANGE %s", rangeName))
		if err := row.Scan(&currentConfig); err != nil {
			// If we can't read the current config, just proceed without tracking
			// This handles cases where the range doesn't have an explicit zone config
			h.logger.Printf("Note: Could not capture original zone config for %s: %v", rangeName, err)
		} else {
			// Register cleanup to restore original zone config (LIFO order)
			h.RegisterCleanup(
				fmt.Sprintf("Restore zone config for RANGE %s", rangeName),
				fmt.Sprintf("ALTER RANGE %s CONFIGURE ZONE USING %s", rangeName, currentConfig),
			)
		}
	}
	return h.Exec(fmt.Sprintf("ALTER RANGE %s CONFIGURE ZONE USING %s", rangeName, zoneConfig))
}

// AlterAllRanges alters zone configuration for all system ranges and the default range.
// This ensures complete coverage:
// - System ranges (meta, system, liveness, timeseries)
// - Default zone (for future tables)
func (h *Helper) AlterAllRanges(zoneConfig string) error {
	// Update all standard system and default ranges
	systemRanges := []string{
		"meta",
		"system",
		"liveness",
		"timeseries",
		"default",
	}

	for _, rangeName := range systemRanges {
		if err := h.AlterRange(rangeName, zoneConfig); err != nil {
			return fmt.Errorf("failed to alter zone config for %s: %w", rangeName, err)
		}
	}

	return nil
}

// InitWorkload initializes a workload (e.g., bank, tpcc, kv) and returns the database name.
// The buildCmd callback allows customizing the command with flags.
// The database name is the same as the workload name.
func (h *Helper) InitWorkload(
	workload string,
	buildCmd func(cmd *roachtestutil.Command) *roachtestutil.Command,
) (string, error) {
	dbName := workload
	node := h.RandomAvailableNode()

	baseCmd := roachtestutil.NewCommand("%s workload init %s", test.DefaultCockroachPath, workload).
		Flag("db", dbName)
	if buildCmd != nil {
		baseCmd = buildCmd(baseCmd)
	}
	cmd := baseCmd.Arg("{pgurl:%d}", node).String()

	if err := h.cluster.RunE(h.ctx, option.WithNodes(h.cluster.WorkloadNode()), cmd); err != nil {
		return "", fmt.Errorf("failed to init workload %s: %w", workload, err)
	}

	h.RegisterCleanup(
		fmt.Sprintf("Drop workload database %s", dbName),
		fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", dbName),
	)

	return dbName, nil
}

// RunWorkload runs a workload asynchronously and returns a cancel function.
// The buildCmd callback allows customizing the command with flags (e.g., duration, concurrency).
func (h *Helper) RunWorkload(
	workload, dbName string,
	buildCmd func(cmd *roachtestutil.Command) *roachtestutil.Command,
) context.CancelFunc {
	node := h.RandomAvailableNode()

	baseCmd := roachtestutil.NewCommand("%s workload run %s", test.DefaultCockroachPath, workload).
		Flag("db", dbName)
	if buildCmd != nil {
		baseCmd = buildCmd(baseCmd)
	}
	cmd := baseCmd.Arg("{pgurl:%d}", node).String()

	return h.GoCommand(cmd, h.cluster.WorkloadNode())
}

// RunWorkloadSync runs a workload synchronously (blocking until completion).
// The buildCmd callback allows customizing the command with flags.
func (h *Helper) RunWorkloadSync(
	workload, dbName string,
	buildCmd func(cmd *roachtestutil.Command) *roachtestutil.Command,
) error {
	node := h.RandomAvailableNode()

	baseCmd := roachtestutil.NewCommand("%s workload run %s", test.DefaultCockroachPath, workload).
		Flag("db", dbName)
	if buildCmd != nil {
		baseCmd = buildCmd(baseCmd)
	}
	cmd := baseCmd.Arg("{pgurl:%d}", node).String()

	return h.cluster.RunE(h.ctx, option.WithNodes(h.cluster.WorkloadNode()), cmd)
}

// ColumnInfo represents information about a table column.
type ColumnInfo struct {
	Name string
	Type uint32 // OID type
}

// PickRandomDatabase picks a random database from the cluster.
func (h *Helper) PickRandomDatabase() (string, error) {
	rows, err := h.Query("SELECT database_name FROM [SHOW DATABASES] WHERE database_name NOT IN ('system', 'postgres', 'defaultdb', 'information_schema')")
	if err != nil {
		return "", fmt.Errorf("failed to query databases: %w", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return "", fmt.Errorf("failed to scan database name: %w", err)
		}
		databases = append(databases, dbName)
	}

	if len(databases) == 0 {
		return "", fmt.Errorf("no user databases found")
	}

	return databases[h.rng.Intn(len(databases))], nil
}

// PickRandomTable picks a random table from the specified database.
func (h *Helper) PickRandomTable(database string) (string, error) {
	query := fmt.Sprintf("SELECT table_name FROM [SHOW TABLES FROM %s]", database)
	rows, err := h.Query(query)
	if err != nil {
		return "", fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return "", fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	if len(tables) == 0 {
		return "", fmt.Errorf("no tables found in database %s", database)
	}

	return tables[h.rng.Intn(len(tables))], nil
}

// GetTableColumns returns column information for the specified table.
func (h *Helper) GetTableColumns(database, table string) ([]ColumnInfo, error) {
	query := fmt.Sprintf(`
SELECT attname, atttypid
FROM pg_catalog.pg_attribute
WHERE attrelid = '%s.%s'::REGCLASS AND attnum > 0 AND NOT attisdropped
ORDER BY attnum`, database, table)

	rows, err := h.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query table columns: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		if err := rows.Scan(&col.Name, &col.Type); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}
		columns = append(columns, col)
	}

	return columns, nil
}

// SearchTable finds a random database and table that satisfies the given predicate.
// It exhaustively searches all database+table combinations, collects matches,
// and returns a random one. This ensures we don't miss valid tables due to randomness.
func (h *Helper) SearchTable(pred func(dbName, tableName string) bool) (string, string, error) {
	// Get all databases using existing helper logic
	rows, err := h.Query("SELECT database_name FROM [SHOW DATABASES] WHERE database_name NOT IN ('system', 'postgres', 'information_schema')")
	if err != nil {
		return "", "", fmt.Errorf("failed to query databases: %w", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return "", "", fmt.Errorf("failed to scan database name: %w", err)
		}
		databases = append(databases, dbName)
	}

	if len(databases) == 0 {
		return "", "", fmt.Errorf("no user databases found")
	}

	// Collect all valid database+table combinations
	type tableMatch struct {
		database string
		table    string
	}
	var matches []tableMatch

	// Exhaustively search all combinations
	for _, dbName := range databases {
		// Get all tables using existing helper logic
		query := fmt.Sprintf("SELECT table_name FROM [SHOW TABLES FROM %s]", dbName)
		tableRows, err := h.Query(query)
		if err != nil {
			continue // Skip databases we can't query
		}

		var tables []string
		for tableRows.Next() {
			var tableName string
			if err := tableRows.Scan(&tableName); err != nil {
				continue
			}
			tables = append(tables, tableName)
		}
		tableRows.Close()

		// Test predicate on each table
		for _, tableName := range tables {
			if pred(dbName, tableName) {
				matches = append(matches, tableMatch{database: dbName, table: tableName})
			}
		}
	}

	if len(matches) == 0 {
		return "", "", fmt.Errorf("no table found matching the predicate")
	}

	// Return a random match from all valid options
	chosen := matches[h.rng.Intn(len(matches))]
	return chosen.database, chosen.table, nil
}

// defaultTaskOptions returns the default options that are passed to all tasks
// started by the helper.
func (h *Helper) defaultTaskOptions() []task.Option {
	loggerFuncOpt := task.LoggerFunc(func(name string) (*logger.Logger, error) {
		bgLogger, err := h.loggerFor(name)
		if err != nil {
			return nil, fmt.Errorf("failed to create logger for task function %q: %w", name, err)
		}
		return bgLogger, nil
	})
	panicOpt := task.PanicHandler(func(_ context.Context, name string, l *logger.Logger, r interface{}) error {
		l.Printf("panic in task function %s: %v", name, r)
		return fmt.Errorf("panic in task function %s: %v", name, r)
	})
	errHandlerOpt := task.ErrorHandler(func(ctx context.Context, name string, l *logger.Logger, err error) error {
		if err != nil {
			if task.IsContextCanceled(ctx) {
				return err
			}
			l.Printf("error in task function %s: %v", name, err)
			return errors.Wrapf(err, "error in task function %s", name)
		}
		return nil
	})
	return []task.Option{loggerFuncOpt, panicOpt, errHandlerOpt}
}

// GoWithCancel implements the Tasker interface.
func (h *Helper) GoWithCancel(fn task.Func, opts ...task.Option) context.CancelFunc {
	return h.background.GoWithCancel(
		fn, task.OptionList(h.defaultTaskOptions()...), task.OptionList(opts...),
	)
}

// Go implements the Tasker interface.
func (h *Helper) Go(fn task.Func, opts ...task.Option) {
	h.GoWithCancel(fn, opts...)
}

// NewGroup implements the Group interface.
func (h *Helper) NewGroup(opts ...task.Option) task.Group {
	return h.background.NewGroup(task.OptionList(h.defaultTaskOptions()...), task.OptionList(opts...))
}

// NewErrorGroup implements the Group interface.
func (h *Helper) NewErrorGroup(opts ...task.Option) task.ErrorGroup {
	return h.background.NewErrorGroup(task.OptionList(h.defaultTaskOptions()...), task.OptionList(opts...))
}

// GoCommand has the same semantics of `GoWithCancel()`; the command passed will
// run and the test will fail if the command is not successful. The task name is
// derived from the command passed.
func (h *Helper) GoCommand(cmd string, nodes option.NodeListOption) context.CancelFunc {
	desc := fmt.Sprintf("run command: %q", cmd)
	return h.GoWithCancel(func(ctx context.Context, l *logger.Logger) error {
		l.Printf("running command `%s` on nodes %v in a task", cmd, nodes)
		return h.cluster.RunE(ctx, option.WithNodes(nodes), cmd)
	}, task.Name(desc))
}

func (h *Helper) RunCleanup(ctx context.Context) error {
	return h.gc.RunCleanup(ctx, h.defaultService.RandomDBConn(h.rng))
}

// loggerFor creates a logger instance to be used by task functions (created by
// calling `Go` on the helper instance). It is similar to the logger instances
// created for mixed-version steps, but with the `task_` prefix.
func (h *Helper) loggerFor(name string) (*logger.Logger, error) {
	atomic.AddInt64(&h.taskCount, 1)

	fileName := invalidChars.ReplaceAllString(strings.ToLower(name), "")
	fileName = fmt.Sprintf("task_%s_%d", fileName, h.taskCount)
	fileName = path.Join(logPrefix, fileName)

	return h.logger.ChildLogger(fileName)
}

// Service implements helper functions on behalf of a specific
// service. Internal fields are provided by the testRunner struct,
// allowing us to connect to a specific node and check live the test
// runner's view of cluster versions, etc.
type Service struct {
	name       string
	ctx        context.Context
	connFunc   func(int) *gosql.DB
	stepLogger *logger.Logger
	monitor    test.Monitor
	cluster    cluster.Cluster
	nodes      option.NodeListOption
}

func (s *Service) AvailableNodes() option.NodeListOption {
	monitorNodes := s.monitor.AvailableNodes(s.name)
	if len(monitorNodes) == 0 {
		// If the monitor doesn't have any nodes tracked yet (e.g., early in test setup),
		// fall back to the configured nodes.
		return s.nodes
	}
	return monitorNodes.Intersect(s.nodes)
}

func (s *Service) RandomAvailableNode(rng *rand.Rand) int {
	nodes := s.AvailableNodes()
	return nodes.SeededRandNode(rng)[0]
}

// Connect returns a connection pool to the given node. Note that
// these connection pools are managed by the framework and therefore
// *must not* be closed. They are closed automatically when the test
// finishes.
func (s *Service) Connect(node int) *gosql.DB {
	return s.connFunc(node)
}

// RandomDBConn returns a connection pool to a random node in the
// cluster. Do *not* call `Close` on the pool returned (see comment on
// `Connect` function).
func (s *Service) RandomDBConn(rng *rand.Rand) *gosql.DB {
	node := s.RandomAvailableNode(rng)
	return s.Connect(node)
}

// RandomDB is like RandomDBConn, but also returns the node ID.
func (s *Service) RandomDB(rng *rand.Rand) (int, *gosql.DB) {
	node := s.RandomAvailableNode(rng)
	return node, s.Connect(node)
}

// prepareQuery returns a connection to one of the available nodes in `nodes`
// provided and logs the query and gateway node in the step's log file. Called
// before the query is actually performed.
func (s *Service) prepareQuery(
	rng *rand.Rand, nodes option.NodeListOption, query string, args ...any,
) (*gosql.DB, error) {
	availableNodes := s.AvailableNodes().Intersect(nodes)
	if len(availableNodes) == 0 {
		return nil, errors.Newf(
			"no available nodes in the intersection of %s and %s",
			s.AvailableNodes(), nodes,
		)
	}
	node := availableNodes.SeededRandNode(rng)[0]
	db := s.Connect(node)

	logSQL(
		s.stepLogger, node, s.name, query, args...,
	)

	return db, nil
}

func (s *Service) Query(rng *rand.Rand, query string, args ...interface{}) (*gosql.Rows, error) {
	db, err := s.prepareQuery(rng, s.nodes, query, args...)
	handleInternalError(err)
	return db.QueryContext(s.ctx, query, args...)
}

func (s *Service) QueryRow(rng *rand.Rand, query string, args ...interface{}) *gosql.Row {
	db, err := s.prepareQuery(rng, s.nodes, query, args...)
	handleInternalError(err)
	return db.QueryRowContext(s.ctx, query, args...)
}

func (s *Service) Exec(rng *rand.Rand, query string, args ...interface{}) error {
	return s.ExecWithGateway(rng, s.nodes, query, args...)
}

func (s *Service) ExecWithGateway(
	rng *rand.Rand, nodes option.NodeListOption, query string, args ...interface{},
) error {
	db, err := s.prepareQuery(rng, nodes, query, args...)
	if err != nil {
		return err
	}

	_, err = db.ExecContext(s.ctx, query, args...)
	return err
}

// logSQL standardizes the logging when a SQL statement or query is
// run using one of the Helper methods. It includes the node used as
// gateway for ease of debugging.
func logSQL(
	l *logger.Logger,
	node int,
	serviceName string,
	stmt string,
	args ...interface{},
) {
	var lines []string
	addLine := func(format string, args ...interface{}) {
		lines = append(lines, fmt.Sprintf(format, args...))
	}

	addLine("running SQL")
	addLine("Node:      %d", node)
	addLine("Service:   %s", serviceName)
	addLine("Statement: %s", stmt)
	addLine("Arguments: %v", args)

	l.Printf("%s", strings.Join(lines, "\n"))
}

// handleInternalError can be used when the caller does not expect any
// errors from a function call. If the error value provided is not
// nil, we'll panic with an internal error message.
func handleInternalError(err error) {
	if err == nil {
		return
	}

	panic(fmt.Errorf("modular internal error: %w", err))
}

func generateRandomName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}
