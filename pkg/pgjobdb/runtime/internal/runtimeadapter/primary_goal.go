package runtimeadapter

// SupportsPrimaryGoalSelection advertises the complete scheduler contract,
// including current authorization and retained accepted receipts.
func (s Scheduler) SupportsPrimaryGoalSelection() bool { return true }
