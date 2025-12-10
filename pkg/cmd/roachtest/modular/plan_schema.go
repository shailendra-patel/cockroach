// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package modular

import "fmt"

// PlanSchema manages the database schema for a single test plan execution.
// Each test plan gets its own schema (e.g., "test_plan_12345") for isolation,
// ensuring that objects created by different test plans don't conflict.
type PlanSchema struct {
	// Name is the unique schema name for this plan (e.g., "test_plan_12345").
	Name string
	// Database is the database where the schema is created (typically "defaultdb").
	Database string
}

// seedToSchemaName converts a seed to a valid SQL schema name.
// Negative seeds are prefixed with "n" (e.g., -12345 -> "test_plan_n12345")
// since SQL identifiers cannot start with a minus sign.
func seedToSchemaName(seed int64) string {
	if seed < 0 {
		return fmt.Sprintf("test_plan_n%d", -seed)
	}
	return fmt.Sprintf("test_plan_%d", seed)
}

// NewPlanSchema creates a new plan schema with a name derived from the seed.
// The seed ensures reproducible schema names for debugging.
func NewPlanSchema(seed int64) *PlanSchema {
	return &PlanSchema{
		Name:     seedToSchemaName(seed),
		Database: "defaultdb",
	}
}

// NewPlanSchemaWithDatabase creates a plan schema in the specified database.
func NewPlanSchemaWithDatabase(seed int64, database string) *PlanSchema {
	return &PlanSchema{
		Name:     seedToSchemaName(seed),
		Database: database,
	}
}

// FullyQualifiedName returns the fully qualified schema name (database.schema).
func (ps *PlanSchema) FullyQualifiedName() string {
	return fmt.Sprintf("%s.%s", ps.Database, ps.Name)
}

// CreateSQL returns the SQL statement to create this schema.
func (ps *PlanSchema) CreateSQL() string {
	return fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s", ps.Database, ps.Name)
}

// DropSQL returns the SQL statement to drop this schema and all contained objects.
// Uses CASCADE to ensure all objects in the schema are dropped.
func (ps *PlanSchema) DropSQL() string {
	return fmt.Sprintf("DROP SCHEMA IF EXISTS %s.%s CASCADE", ps.Database, ps.Name)
}

// QualifyTable returns a fully qualified table name within this schema.
func (ps *PlanSchema) QualifyTable(tableName string) string {
	return fmt.Sprintf("%s.%s.%s", ps.Database, ps.Name, tableName)
}

// QualifyIndex returns a fully qualified index name within this schema.
func (ps *PlanSchema) QualifyIndex(indexName string) string {
	return fmt.Sprintf("%s.%s.%s", ps.Database, ps.Name, indexName)
}

// SetSearchPathSQL returns the SQL statement to set the search_path to this schema.
// Includes "public" as a fallback for system tables.
func (ps *PlanSchema) SetSearchPathSQL() string {
	return fmt.Sprintf("SET search_path = %s, public", ps.Name)
}

// ResetSearchPathSQL returns the SQL statement to reset the search_path to default.
func (ps *PlanSchema) ResetSearchPathSQL() string {
	return "SET search_path = public"
}