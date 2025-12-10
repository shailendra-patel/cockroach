package modular

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"path"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
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

// Helper provides utilities for modular test steps.
type Helper struct {
	defaultService *Service
	rng            *rand.Rand
	// taskCount keeps track of the number of tasks started with `helper.Go()`.
	// The counter is used to generate unique log file names.
	taskCount    int64
	cluster      cluster.Cluster
	logger       *logger.Logger
	background   task.Manager
	ctx          context.Context
	stateTracker *ClusterStateTracker
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

// Connect returns a connection pool to the given node using the default service.
func (h *Helper) Connect(node int) *gosql.DB {
	return h.defaultService.Connect(node)
}

// RandomDBConn returns a connection pool to a random node using the default service.
func (h *Helper) RandomDBConn() *gosql.DB {
	return h.defaultService.RandomDBConn(h.rng)
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
func (h *Helper) Exec(query string, args ...interface{}) error {
	return h.defaultService.Exec(h.rng, query, args...)
}

// ExecWithGateway is like Exec, but allows the caller to specify the
// set of nodes that should be used as gateway.
func (h *Helper) ExecWithGateway(
	nodes option.NodeListOption, query string, args ...interface{},
) error {
	return h.defaultService.ExecWithGateway(h.rng, nodes, query, args...)
}

// CreateTable creates a table with the specified schema.
func (h *Helper) CreateTable(namePrefix, schema string) (string, error) {
	tableName := h.stateTracker.NewTableName(namePrefix)
	query := fmt.Sprintf("CREATE TABLE %s (%s)", tableName, schema)
	return tableName, h.Exec(query)
}

// SetClusterSetting sets a cluster setting and registers restoration on cleanup.
// Cluster settings are global and cannot be scoped to a schema, so we capture
// the original value and register a cleanup statement to restore it.
func (h *Helper) SetClusterSetting(settingName, newValue string) error {
	// Capture the current value for restoration during cleanup
	if h.gc != nil && h.gc.Enabled() {
		var currentValue string
		row := h.QueryRow(fmt.Sprintf("SHOW CLUSTER SETTING %s", settingName))
		if err := row.Scan(&currentValue); err != nil {
			return fmt.Errorf("failed to read current value of %s: %w", settingName, err)
		}

		// Register cleanup to restore original value (LIFO order)
		h.gc.Stack().PushSQL(
			fmt.Sprintf("Restore cluster setting %s to '%s'", settingName, currentValue),
			fmt.Sprintf("SET CLUSTER SETTING %s = '%s'", settingName, currentValue),
		)
	}

	// Use parameterized query for the value but format the setting name
	query := fmt.Sprintf("SET CLUSTER SETTING %s = $1", settingName)
	return h.Exec(query, newValue)
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
		h.gc.Stack().PushSQL(
			fmt.Sprintf("Restore cluster setting %s to '%s'", settingName, currentValue),
			fmt.Sprintf("SET CLUSTER SETTING %s = '%s'", settingName, currentValue),
		)
	}
	return h.Exec(fmt.Sprintf("RESET CLUSTER SETTING %s", settingName))
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
			h.gc.Stack().PushSQL(
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

func (h *Helper) CreateUser(namePrefix string, args ...string) (string, error) {
	username := h.stateTracker.NewUsername(namePrefix)
	query := fmt.Sprintf("CREATE USER %s %s", username, joinArgs(args...))
	return username, h.Exec(query)
}

func (h *Helper) CreateUserPassword(namePrefix, password string, args ...string) (string, error) {
	args = append([]string{fmt.Sprintf("WITH PASSWORD %s", password)}, args...)
	return h.CreateUser(namePrefix, args...)
}

// TODO: InjectFailure

// CreateDatabase creates a database with automatic name generation and tracking.
// Since databases are external to the plan schema, cleanup is registered.
func (h *Helper) CreateDatabase(namePrefix string, args ...string) (string, error) {
	dbName := h.stateTracker.NewDatabaseName(namePrefix)
	query := fmt.Sprintf("CREATE DATABASE %s %s", dbName, joinArgs(args...))
	if err := h.Exec(strings.TrimSpace(query)); err != nil {
		return "", err
	}

	// Register cleanup for external database
	if h.gc != nil && h.gc.Enabled() {
		h.gc.Stack().PushSQL(
			fmt.Sprintf("Drop database %s", dbName),
			fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", dbName),
		)
	}

	return dbName, nil
}

// RegisterCleanup allows registering a custom cleanup statement.
// Use this for objects created outside of standard Helper methods,
// such as databases created by external workloads (TPCC, YCSB, etc.).
func (h *Helper) RegisterCleanup(description, statement string) {
	if h.gc != nil && h.gc.Enabled() {
		h.gc.Stack().PushSQL(description, statement)
	}
}

// GC returns the garbage collector for direct access if needed.
// Use with caution; prefer using Helper methods that auto-register cleanup.
func (h *Helper) GC() *GarbageCollector {
	return h.gc
}

// CreateSchema creates a schema with automatic name generation and tracking.
func (h *Helper) CreateSchema(namePrefix string, args ...string) (string, error) {
	schemaName := h.stateTracker.NewSchemaName(namePrefix)
	query := fmt.Sprintf("CREATE SCHEMA %s %s", schemaName, joinArgs(args...))
	return schemaName, h.Exec(strings.TrimSpace(query))
}

// CreateIndex creates an index with automatic name generation and tracking.
func (h *Helper) CreateIndex(namePrefix, database, table string, columns []string, args ...string) (string, error) {
	indexName := h.stateTracker.NewIndexName(namePrefix)
	columnsStr := strings.Join(columns, ", ")
	tableRef := fmt.Sprintf("%s.%s", database, table)
	query := fmt.Sprintf("CREATE INDEX %s ON %s (%s) %s", indexName, tableRef, columnsStr, joinArgs(args...))
	return indexName, h.Exec(strings.TrimSpace(query))
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
