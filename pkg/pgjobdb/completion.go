package pgjobdb

// CompletionStatus represents the outcome stored in pgjobdb.jobs_archive.
// Values are arbitrary, but empty defaults to "succeeded" when completing jobs.
type CompletionStatus string

const (
	CompletionStatusSucceeded CompletionStatus = "succeeded"
	CompletionStatusFailed    CompletionStatus = "failed"
	CompletionStatusCancelled CompletionStatus = "cancelled"
)

func normalizeCompletion(status CompletionStatus, completionDetail string) (CompletionStatus, any) {
	if status == "" {
		status = CompletionStatusSucceeded
	}
	if completionDetail == "" {
		return status, nil
	}
	return status, completionDetail
}
