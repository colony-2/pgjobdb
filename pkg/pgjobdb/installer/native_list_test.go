package installer_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
)

func TestNativeListJobsFiltersBeforePagination(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		for _, item := range []struct {
			tenant  pgjobdb.TenantID
			id      pgjobdb.JobID
			parent  pgjobdb.JobID
			jobType pgjobdb.JobType
			source  string
		}{
			{"tenant", "root", "", "collect", "api"},
			{"tenant", "child", "root", "collect", "batch"},
			{"tenant", "task", "root", "collect", "api"},
			{"tenant", "archived", "root", "collect", "api"},
			{"tenant", "unrelated", "", "cleanup", "other"},
			{"other-tenant", "other", "", "collect", "api"},
		} {
			metadata, _ := json.Marshal(map[string]string{"source": item.source})
			if _, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{
				TenantID: item.tenant, JobID: item.id, WorkerID: "submitter",
				JobType: item.jobType, AppMetadata: metadata,
				Runtime: pgjobdb.RuntimeMetadata{ParentJobID: item.parent},
			}); err != nil {
				t.Fatalf("submit %s: %v", item.id, err)
			}
		}
		if err := pgjobdb.RescheduleUnheldJob(ctx, db, "tenant", "task", "worker",
			pgjobdb.RescheduleRequest{
				RouteJobType: "collect", WorkKind: pgjobdb.WorkKindTask,
				Task: &pgjobdb.TaskWork{
					TaskType: "download", ResumeJobType: "collect",
					InputOrdinal: 1, OutputOrdinal: 2, InputHash: "sha256:input",
				},
			}); err != nil {
			t.Fatalf("route task: %v", err)
		}
		if err := pgjobdb.CompleteUnheldJob(ctx, db, "tenant", "archived", "worker",
			pgjobdb.Completion{Status: pgjobdb.CompletionSuccess}); err != nil {
			t.Fatalf("archive job: %v", err)
		}
		base := pgjobdb.ListJobsOptions{TenantIDs: []pgjobdb.TenantID{"tenant"}}
		all, err := pgjobdb.ListJobs(ctx, db, base)
		if err != nil || len(all.Jobs) != 5 {
			t.Fatalf("all tenant jobs = %+v, %v", all, err)
		}
		jobTypeFilter := base
		jobTypeFilter.JobTypes = []pgjobdb.JobType{"collect"}
		byType, err := pgjobdb.ListJobs(ctx, db, jobTypeFilter)
		if err != nil || len(byType.Jobs) != 4 {
			t.Fatalf("collect jobs = %+v, %v", byType, err)
		}
		roots := base
		roots.RootOnly = true
		rootResult, err := pgjobdb.ListJobs(ctx, db, roots)
		if err != nil || len(rootResult.Jobs) != 2 {
			t.Fatalf("root jobs = %+v, %v", rootResult, err)
		}
		parent := base
		parent.ParentJobIDs = []pgjobdb.JobID{"root"}
		parent.PageSize = 1
		var listed []pgjobdb.JobID
		for {
			page, err := pgjobdb.ListJobs(ctx, db, parent)
			if err != nil || len(page.Jobs) != 1 {
				t.Fatalf("parent page = %+v, %v", page, err)
			}
			listed = append(listed, page.Jobs[0].JobID)
			if page.NextPageToken == "" {
				break
			}
			parent.PageToken = page.NextPageToken
		}
		if len(listed) != 3 || listed[0] != "archived" ||
			listed[1] != "task" || listed[2] != "child" {
			t.Fatalf("parent pages = %v", listed)
		}
		if _, err := pgjobdb.ListJobs(ctx, db, pgjobdb.ListJobsOptions{
			TenantIDs: []pgjobdb.TenantID{"tenant"}, RootOnly: true,
			PageToken: parent.PageToken,
		}); err == nil {
			t.Fatal("expected page token filter mismatch")
		}
		taskFilter := base
		taskFilter.TaskSelectors = []pgjobdb.TaskSelector{
			{JobType: "collect", TaskType: "download"},
		}
		tasks, err := pgjobdb.ListJobs(ctx, db, taskFilter)
		if err != nil || len(tasks.Jobs) != 1 || tasks.Jobs[0].JobID != "task" {
			t.Fatalf("task jobs = %+v, %v", tasks, err)
		}
		archiveFilter := base
		archiveFilter.Statuses = []pgjobdb.JobStatus{pgjobdb.JobStatusCompleted}
		archiveFilter.Stores = []pgjobdb.JobStore{pgjobdb.JobStoreArchived}
		archives, err := pgjobdb.ListJobs(ctx, db, archiveFilter)
		if err != nil || len(archives.Jobs) != 1 || archives.Jobs[0].JobID != "archived" {
			t.Fatalf("archived jobs = %+v, %v", archives, err)
		}
		keyFilter := base
		keyFilter.JobKeys = []pgjobdb.JobKey{{TenantID: "tenant", JobID: "child"}}
		keys, err := pgjobdb.ListJobs(ctx, db, keyFilter)
		if err != nil || len(keys.Jobs) != 1 || keys.Jobs[0].JobID != "child" {
			t.Fatalf("key jobs = %+v, %v", keys, err)
		}
		metadataFilter := base
		metadataFilter.MetadataPredicates = []pgjobdb.MetadataPredicate{
			{Path: []string{"source"}, Values: []json.RawMessage{json.RawMessage(`"api"`)}},
		}
		metadataJobs, err := pgjobdb.ListJobs(ctx, db, metadataFilter)
		if err != nil || len(metadataJobs.Jobs) != 3 {
			t.Fatalf("metadata jobs = %+v, %v", metadataJobs, err)
		}
		metadataFilter.MetadataPredicates[0].Values = []json.RawMessage{
			json.RawMessage(`"api"`), json.RawMessage(`"batch"`),
		}
		metadataJobs, err = pgjobdb.ListJobs(ctx, db, metadataFilter)
		if err != nil || len(metadataJobs.Jobs) != 4 {
			t.Fatalf("metadata OR jobs = %+v, %v", metadataJobs, err)
		}
	})
}
