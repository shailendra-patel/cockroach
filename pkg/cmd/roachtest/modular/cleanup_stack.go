// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package modular

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

// CleanupEntry represents a single cleanup action to be executed.
type CleanupEntry struct {
	// Statement is the SQL statement to execute for cleanup.
	Statement string
	// Description is a human-readable description for logging.
	Description string
	// ContinueOnError indicates whether to continue cleanup even if this statement fails.
	ContinueOnError bool
}

// CleanupStack is a thread-safe LIFO stack of cleanup entries.
// Cleanup entries are pushed when objects are created and popped during cleanup,
// ensuring proper dependency ordering (last created = first deleted).
type CleanupStack struct {
	mu      sync.Mutex
	entries []CleanupEntry
	logger  *logger.Logger
}

// NewCleanupStack creates a new cleanup stack with the given logger.
func NewCleanupStack(l *logger.Logger) *CleanupStack {
	return &CleanupStack{
		entries: make([]CleanupEntry, 0),
		logger:  l,
	}
}

// Push adds a cleanup entry to the top of the stack.
func (cs *CleanupStack) Push(entry CleanupEntry) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.entries = append(cs.entries, entry)
	cs.logger.Printf("[GC] Registered cleanup: %s", entry.Description)
}

// PushSQL is a convenience method for pushing a SQL cleanup statement.
// By default, cleanup continues even if the statement fails.
func (cs *CleanupStack) PushSQL(description, statement string) {
	cs.Push(CleanupEntry{
		Statement:       statement,
		Description:     description,
		ContinueOnError: true,
	})
}

// PushSQLStrict is like PushSQL but stops cleanup if the statement fails.
func (cs *CleanupStack) PushSQLStrict(description, statement string) {
	cs.Push(CleanupEntry{
		Statement:       statement,
		Description:     description,
		ContinueOnError: false,
	})
}

// Pop removes and returns the top entry from the stack.
// Returns nil if the stack is empty.
func (cs *CleanupStack) Pop() *CleanupEntry {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if len(cs.entries) == 0 {
		return nil
	}
	entry := cs.entries[len(cs.entries)-1]
	cs.entries = cs.entries[:len(cs.entries)-1]
	return &entry
}

// Size returns the number of entries in the stack.
func (cs *CleanupStack) Size() int {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return len(cs.entries)
}

// Clear removes all entries from the stack without executing them.
func (cs *CleanupStack) Clear() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.entries = cs.entries[:0]
}

// Entries returns a copy of all entries in the stack (for inspection/debugging).
// The returned slice is ordered from bottom to top (first pushed to last pushed).
func (cs *CleanupStack) Entries() []CleanupEntry {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	result := make([]CleanupEntry, len(cs.entries))
	copy(result, cs.entries)
	return result
}

// IsEmpty returns true if the stack has no entries.
func (cs *CleanupStack) IsEmpty() bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return len(cs.entries) == 0
}
