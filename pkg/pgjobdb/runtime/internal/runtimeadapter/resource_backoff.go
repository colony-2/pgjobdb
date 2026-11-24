package runtimeadapter

import (
	"context"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
)

var _ runtimecore.ResourceBackoffScheduler = Scheduler{}

func (s Scheduler) ResourceBackoffCapability(ctx context.Context) (jobdb.ResourceBackoffCapability, error) {
	if s.Chapters == nil {
		return jobdb.ResourceBackoffCapability{}, &jobdb.ResourceBackoffUnsupportedError{Feature: "immutable chapter log"}
	}
	if err := pgjobdb.VerifyResourceBackoff(ctx, s.DB); err != nil {
		return jobdb.ResourceBackoffCapability{}, err
	}
	return jobdb.ResourceBackoffCapability{ProtocolVersion: 1, DurabilityVersion: 1}, nil
}
func (s Scheduler) ReplayResourceBackoffReceipt(ctx context.Context, mutation runtimecore.ResourceBackoffMutation) (jobdb.ResourceBackoffReceipt, error) {
	return pgjobdb.ReplayResourceBackoffReceipt(ctx, s.DB, identityToPgjobdb(mutation.Identity), mutation.Request)
}
func (s Scheduler) RescheduleResourceBackoff(ctx context.Context, mutation runtimecore.ResourceBackoffMutation) (jobdb.ResourceBackoffReceipt, error) {
	return pgjobdb.RescheduleResourceBackoff(ctx, s.DB, identityToPgjobdb(mutation.Identity), mutation.Request, pgjobdb.ResourceBackoffOptions{
		Sample: s.ResourceSample, Fault: s.ResourceFault,
		ValidateCheckpoint: func(ctx context.Context, req jobdb.ResourceBackoffRequest) (time.Time, error) {
			chapter, err := s.GetChapter(ctx, jobdb.ChapterRef{JobKey: mutation.Identity.JobKey, Ordinal: req.Resume.CheckpointOrdinal})
			if err != nil {
				return time.Time{}, err
			}
			if err = jobdb.ValidateResourceCheckpoint(chapter, req); err != nil {
				return time.Time{}, err
			}
			if w := req.Resume.TaskWait; w != nil {
				input, err := s.GetChapter(ctx, jobdb.ChapterRef{JobKey: mutation.Identity.JobKey, Ordinal: w.InputOrdinal})
				if err != nil {
					return time.Time{}, err
				}
				if err = jobdb.ValidateResourceTaskCheckpoint(chapter, input, req); err != nil {
					return time.Time{}, err
				}
				// Public read-only validation computes the declared input identity. No
				// output is committed and the inert data contains no artifacts to load.
				_, err = runtimecore.ExecutionCompletion(ctx, s, jobdb.CompleteTaskIfWaitingRequest{JobKey: mutation.Identity.JobKey, InputOrdinal: w.InputOrdinal, OutputOrdinal: w.OutputOrdinal, InputHash: w.InputHash, Data: jobdb.NewTaskDataOrPanic(nil)}, w.InputOrdinal)
				if err != nil {
					return time.Time{}, err
				}
			}
			record, err := jobdb.DecodeExecutionRecord(chapter.Body.(jobdb.ExecutionChapter).Record)
			if err != nil {
				return time.Time{}, err
			}
			deadline := record.Deadline
			if !record.TotalDeadline.IsZero() && (deadline.IsZero() || record.TotalDeadline.Before(deadline)) {
				deadline = record.TotalDeadline
			}
			return deadline, nil
		},
	})
}
func (s Scheduler) GetChapter(ctx context.Context, ref jobdb.ChapterRef) (jobdb.Chapter, error) {
	if s.Chapters == nil {
		return jobdb.Chapter{}, fmt.Errorf("immutable chapter log unavailable")
	}
	encoded, err := s.Chapters.Get(ctx, runtimecore.ChapterLogKey{JobKey: ref.JobKey}, ref.Ordinal)
	if err != nil {
		return jobdb.Chapter{}, err
	}
	return runtimecore.DecodeChapter(encoded)
}

func (s Scheduler) ListChapters(ctx context.Context, req jobdb.ListChaptersRequest) ([]jobdb.Chapter, error) {
	records, err := s.Chapters.List(ctx, runtimecore.ChapterLogKey{JobKey: req.JobKey}, runtimecore.ChapterRange{})
	if err != nil {
		return nil, err
	}
	chapters := make([]jobdb.Chapter, 0, len(records))
	for _, record := range records {
		ch, err := runtimecore.DecodeChapter(record)
		if err != nil {
			return nil, err
		}
		chapters = append(chapters, ch)
	}
	return chapters, nil
}
