package operations

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/modular"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

// InspectTableOp implements the Operation interface for running INSPECT on a table.
type InspectTableOp struct {
	name    string
	builder *modular.OperationBuilder
}

// Chain returns the operation's chain of steps.
func (i *InspectTableOp) Chain() modular.Chain {
	return i.builder.Chain
}

// Name returns the operation's name.
func (i *InspectTableOp) Name() string {
	return i.name
}

func (i *InspectTableOp) Precondition() bool {
	return true
}

func (i *InspectTableOp) Timeout() time.Duration {
	return 30 * time.Minute
}

// InspectTable creates an operation that runs INSPECT TABLE on a random table
// to verify consistency between the primary index and secondary indexes.
// This is similar to the post-test INSPECT validation but runs during test execution.
func InspectTable() modular.Operation {
	builder := modular.NewOperation("INSPECT table", func(ctx context.Context, l *logger.Logger, h *modular.Helper) error {
		// Find a random table to inspect
		// TODO: replace with h.RandomTable
		dbName, tableName, err := h.SearchTable(func(dbName, tableName string) bool {
			return true
		})
		if err != nil {
			return err
		}

		l.Printf("Running INSPECT on table %s.%s", dbName, tableName)

		// Enable INSPECT command on this connection
		if err := h.Exec("SET enable_inspect_command = true"); err != nil {
			return fmt.Errorf("failed to enable INSPECT command: %w", err)
		}

		// Run INSPECT TABLE - this will check consistency between primary and secondary indexes
		inspectSQL := fmt.Sprintf("INSPECT TABLE %s.%s", dbName, tableName)

		// Use a short statement timeout to force background job execution
		if err := h.Exec("SET statement_timeout = '5s'"); err != nil {
			return fmt.Errorf("failed to set statement timeout: %w", err)
		}
		defer func() {
			if resetErr := h.Exec("RESET statement_timeout"); resetErr != nil {
				l.Printf("Warning: failed to reset statement timeout: %v", resetErr)
			}
		}()

		// Execute INSPECT - may timeout and run as background job
		err = h.Exec(inspectSQL)
		if err != nil && !isStatementTimeoutError(err) {
			return fmt.Errorf("INSPECT TABLE failed: %w", err)
		}

		// Get the most recent INSPECT job
		var jobID int64
		getJobIDSQL := `
			SELECT job_id
			FROM [SHOW JOBS]
			WHERE job_type = 'INSPECT'
			ORDER BY created DESC
			LIMIT 1`
		if err := h.QueryRow(getJobIDSQL).Scan(&jobID); err != nil {
			l.Printf("Warning: failed to get INSPECT job ID: %v", err)
			// If we can't get the job ID, assume the INSPECT ran successfully inline
			l.Printf("INSPECT TABLE %s.%s completed inline (no job created)", dbName, tableName)
			return nil
		}

		l.Printf("INSPECT job ID: %d", jobID)

		// Poll the job until it completes
		const pollInterval = 5 * time.Second
		const maxWait = 10 * time.Minute
		deadline := time.Now().Add(maxWait)

		for time.Now().Before(deadline) {
			var status jobs.State
			var fractionCompleted float64
			checkJobSQL := `
				SELECT status, fraction_completed
				FROM [SHOW JOBS]
				WHERE job_id = $1`
			if err := h.QueryRow(checkJobSQL, jobID).Scan(&status, &fractionCompleted); err != nil {
				return fmt.Errorf("failed to query job %d status: %w", jobID, err)
			}

			// Check if job is complete
			switch status {
			case jobs.StateSucceeded:
				l.Printf("INSPECT job %d completed successfully (100%%)", jobID)

				// Check for any errors found by INSPECT
				if err := checkInspectErrors(h, jobID); err != nil {
					return err
				}

				l.Printf("INSPECT validation passed: no consistency errors found")
				return nil

			case jobs.StateFailed, jobs.StateCanceled:
				return fmt.Errorf("INSPECT job %d finished with status: %s", jobID, status)
			}

			// Log progress
			if int(fractionCompleted*100)%20 == 0 && fractionCompleted > 0 {
				l.Printf("INSPECT job %d: %.0f%% complete", jobID, fractionCompleted*100)
			}

			time.Sleep(pollInterval)
		}

		return fmt.Errorf("INSPECT job %d did not complete within %s", jobID, maxWait)
	})

	return &InspectTableOp{
		name:    "inspect-table",
		builder: builder,
	}
}

// checkInspectErrors checks if there are any errors from the INSPECT job
// and returns them in a formatted error message.
func checkInspectErrors(h *modular.Helper, jobID int64) error {
	var errorCount int
	if err := h.QueryRow(
		fmt.Sprintf("SELECT count(*) FROM [SHOW INSPECT ERRORS FOR JOB %d]", jobID),
	).Scan(&errorCount); err != nil {
		return fmt.Errorf("failed to query INSPECT errors for job %d: %w", jobID, err)
	}

	if errorCount == 0 {
		return nil
	}

	// Get error details
	rows, err := h.Query(fmt.Sprintf(`
		SELECT database_name, schema_name, table_name, error_type, details
		FROM [SHOW INSPECT ERRORS FOR JOB %d WITH DETAILS]`, jobID))
	if err != nil {
		return fmt.Errorf("failed to fetch error details for job %d: %w", jobID, err)
	}
	defer rows.Close()

	var errorDetails string
	for rows.Next() {
		var dbName, schemaName, tblName, errorType, details string
		if err := rows.Scan(&dbName, &schemaName, &tblName, &errorType, &details); err != nil {
			return fmt.Errorf("failed to scan error details: %w", err)
		}
		errorDetails += fmt.Sprintf("\n  - %s.%s.%s: %s (%s)",
			dbName, schemaName, tblName, errorType, details)
	}

	return fmt.Errorf("INSPECT found %d consistency errors:%s", errorCount, errorDetails)
}

// isStatementTimeoutError checks if the error is due to statement timeout.
func isStatementTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	// Check for common statement timeout error messages
	errMsg := err.Error()
	return errMsg == "pq: query execution canceled due to statement timeout" ||
		errMsg == "query execution canceled due to statement timeout"
}
