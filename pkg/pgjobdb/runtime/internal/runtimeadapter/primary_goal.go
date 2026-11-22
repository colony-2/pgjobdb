package runtimeadapter

import "github.com/colony-2/jobdb/pkg/jobdb"

// SupportsPrimaryGoalSelection advertises the complete scheduler contract,
// including current authorization and retained accepted receipts.
func (s Scheduler) SupportsPrimaryGoalSelection() bool { return true }

// JobGoalCapabilities certifies scheduler new-write guarantees, not deployment.
func (s Scheduler) JobGoalCapabilities() jobdb.JobGoalCapabilities {
	return jobdb.JobGoalCapabilities{ImmutableMembership: true, AtomicAdmission: true, TerminalFailureRecords: true}
}
