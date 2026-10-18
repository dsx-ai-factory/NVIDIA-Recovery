// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package job

import (
	"context"
	"testing"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
)

func TestJobsSyntheticSource_PaginationTerminates(t *testing.T) {
	source := NewJobsSyntheticSource()

	tests := []struct {
		name      string
		wantCount int
		getPage   func(*string) ([]string, *string, error)
	}{
		{
			name:      "jobs",
			wantCount: 10,
			getPage: func(token *string) ([]string, *string, error) {
				response, err := source.GetJobs(context.Background(), &pb.GetJobsRequest{Limit: 2, PageToken: token})
				if err != nil {
					return nil, nil, err
				}
				ids := make([]string, len(response.GetJobs()))
				for i, job := range response.GetJobs() {
					ids[i] = job.HeimdallJobId
				}
				return ids, response.NextPageToken, err
			},
		},
		{
			name:      "workflows",
			wantCount: 3,
			getPage: func(token *string) ([]string, *string, error) {
				response, err := source.GetWorkflows(context.Background(), &pb.GetWorkflowsRequest{Limit: 2, PageToken: token})
				if err != nil {
					return nil, nil, err
				}
				ids := make([]string, len(response.GetWorkflows()))
				for i, workflow := range response.GetWorkflows() {
					ids[i] = workflow.WorkflowId
				}
				return ids, response.NextPageToken, err
			},
		},
		{
			name:      "scheduler jobs",
			wantCount: 3,
			getPage: func(token *string) ([]string, *string, error) {
				response, err := source.GetJobsBySchedulerId(context.Background(), &pb.GetJobsBySchedulerIdRequest{
					SchedulerJobId: "12345",
					Limit:          2,
					Page:           token,
				})
				if err != nil {
					return nil, nil, err
				}
				ids := make([]string, len(response.GetJobs()))
				for i, job := range response.GetJobs() {
					ids[i] = job.HeimdallJobId
				}
				return ids, response.NextPageToken, err
			},
		},
		{
			name:      "advice",
			wantCount: 5,
			getPage: func(token *string) ([]string, *string, error) {
				response, err := source.ListAdvice(context.Background(), &pb.ListAdviceRequest{Limit: 2, Page: token})
				if err != nil {
					return nil, nil, err
				}
				ids := make([]string, len(response.GetAdvice()))
				for i, advice := range response.GetAdvice() {
					ids[i] = advice.HeimdallJobId
				}
				return ids, response.NextPageToken, err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			seenIDs := make(map[string]struct{})
			seenTokens := make(map[string]struct{})
			var token *string

			for page := 0; page <= test.wantCount; page++ {
				ids, nextToken, err := test.getPage(token)
				if err != nil {
					t.Fatalf("get page %d: %v", page+1, err)
				}
				if len(ids) > 2 {
					t.Fatalf("page %d returned %d records, want at most 2", page+1, len(ids))
				}
				for _, id := range ids {
					if _, duplicate := seenIDs[id]; duplicate {
						t.Fatalf("record %q appeared on more than one page", id)
					}
					seenIDs[id] = struct{}{}
				}
				if nextToken == nil {
					token = nil
					break
				}
				if _, duplicate := seenTokens[*nextToken]; duplicate {
					t.Fatalf("page token %q repeated", *nextToken)
				}
				seenTokens[*nextToken] = struct{}{}
				token = nextToken
			}

			if token != nil {
				t.Fatalf("pagination did not terminate; last token %q", *token)
			}
			if len(seenIDs) != test.wantCount {
				t.Fatalf("saw %d unique records, want %d", len(seenIDs), test.wantCount)
			}
		})
	}
}

func TestJobsSyntheticSource_PaginationUsesOneBasedPageNumbers(t *testing.T) {
	source := NewJobsSyntheticSource()
	pageTwo := int32(2)

	jobs, err := source.GetJobs(context.Background(), &pb.GetJobsRequest{Limit: 2, PageNumber: &pageTwo})
	if err != nil {
		t.Fatalf("GetJobs: %v", err)
	}
	if len(jobs.Jobs) != 2 || jobs.Jobs[0].HeimdallJobId != "job-2-2" {
		t.Fatalf("jobs on page two = %+v, want first ID job-2-2", jobs.Jobs)
	}

	workflows, err := source.GetWorkflows(context.Background(), &pb.GetWorkflowsRequest{Limit: 2, PageNumber: &pageTwo})
	if err != nil {
		t.Fatalf("GetWorkflows: %v", err)
	}
	if len(workflows.Workflows) != 1 || workflows.Workflows[0].WorkflowId != "workflow-2" {
		t.Fatalf("workflows on page two = %+v, want workflow-2", workflows.Workflows)
	}
}

func TestJobsSyntheticSource_PaginationAppliesFiltersBeforeSlicing(t *testing.T) {
	source := NewJobsSyntheticSource()

	jobs, err := source.GetJobs(context.Background(), &pb.GetJobsRequest{
		Limit:  10,
		JobIds: []string{"job-3-0"},
	})
	if err != nil {
		t.Fatalf("GetJobs by ID: %v", err)
	}
	if len(jobs.Jobs) != 1 || jobs.Jobs[0].HeimdallJobId != "job-3-0" {
		t.Fatalf("jobs filtered by ID = %+v, want job-3-0", jobs.Jobs)
	}

	jobs, err = source.GetJobs(context.Background(), &pb.GetJobsRequest{
		Limit:       10,
		WorkflowIds: []string{"workflow-1"},
	})
	if err != nil {
		t.Fatalf("GetJobs by workflow: %v", err)
	}
	wantIDs := []string{"job-1-1", "job-4-1", "job-7-1"}
	if len(jobs.Jobs) != len(wantIDs) {
		t.Fatalf("got %d workflow jobs, want %d", len(jobs.Jobs), len(wantIDs))
	}
	for i, wantID := range wantIDs {
		if jobs.Jobs[i].HeimdallJobId != wantID {
			t.Fatalf("jobs[%d] = %q, want %q", i, jobs.Jobs[i].HeimdallJobId, wantID)
		}
	}
}
