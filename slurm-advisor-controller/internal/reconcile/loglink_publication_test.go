// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/loglinks"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/state"
)

// This proves publication and filesystem creation agree through SyncOnce and
// the real materializer, including a source extension the collector never tails.
// Accounting must publish the same name after the job leaves the active queue.
func TestControllerPublishesCollectorPathsForActiveAndFinalAttempts(t *testing.T) {
	directory := t.TempDir()
	source, destination := filepath.Join(directory, "sources"), filepath.Join(directory, "links")
	for _, path := range []string{source, destination} {
		if err := os.Mkdir(path, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(source, "output.txt")
	if err := os.WriteFile(path, []byte("training output\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	submit := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	job := logLinkJob("42", 0, "", true, []string{path})
	job.JobName, job.WorkflowID, job.State, job.SubmitTime = "train", "workflow-42", cluster.JobStateRunning, &submit
	job.SchedulerMetadata["scheduler"] = "slurm"
	cfg := loglinks.Config{
		RenameEnabled: true, Destination: destination, SourceRoots: []string{source},
		ScanInterval: time.Minute, CleanupInterval: time.Hour, Retention: 24 * time.Hour, ScanTimeout: time.Second,
		MaxPathLength: 4096, MaxPathsPerSync: 256, MaxPatterns: 32,
		MaxScanDirectories: 64, MaxEntriesPerDirectory: 4096, MaxMatchesPerScan: 512, MaxOwnedLinks: 1024,
	}
	syncer, _, err := loglinks.New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	processCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &fakeControllerClient{
		getJobsResponses:    map[string]*pb.GetJobsResponse{"": {}},
		listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {}},
	}
	reader := newMockJobReader(t)
	expectActiveJobs(reader, []cluster.JobExecutionRecord{job})
	materializer := NewAsyncLogLinkMaterializer(processCtx, syncer)
	controller := Controller{
		JobReader: reader, HeimdallClient: client, SeenJobs: state.NewMemorySeenJobStore(),
		LogLinkNaming: &cfg, LogLinkMaterializer: materializer,
		ModePolicy: ModePolicy{DefaultMode: cluster.JobModeObserve},
	}
	if _, err := controller.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(client.upsertRequests) != 1 {
		t.Fatalf("upserts = %d, want 1", len(client.upsertRequests))
	}
	metadata := client.upsertRequests[0].SchedulerMetadata.AsMap()
	paths := metadata["slurm_log_file_paths"].([]any)
	if len(paths) != 1 {
		t.Fatalf("published paths = %v", paths)
	}
	link := paths[0].(string)
	if filepath.Dir(link) != destination || filepath.Ext(link) != ".log" {
		t.Fatalf("published path = %q, want collector .log in destination", link)
	}
	if got := metadata["app_log_link_targets"].(map[string]any)[link]; got != path {
		t.Fatalf("source mapping = %v, want %q", got, path)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		target, err := os.Readlink(link)
		if err == nil {
			if target != path {
				t.Fatalf("link target = %q, want %q", target, path)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("published link was not materialized: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	if observation := drainCompletedLogLinks(t, materializer); observation.Observation.Errors != 0 {
		t.Fatalf("materialization failed: %+v", observation)
	}
	// The scheduler snapshot and source cache must remain source paths.
	if !reflect.DeepEqual(job.SchedulerMetadata["slurm_log_file_paths"], []string{path}) {
		t.Fatal("publication mutated scheduler metadata")
	}
	job.State = cluster.JobStateCompleted
	if err := controller.upsertJob(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	finalMetadata := client.upsertRequests[1].SchedulerMetadata.AsMap()
	if !reflect.DeepEqual(finalMetadata["slurm_log_file_paths"], paths) {
		t.Fatalf("final accounting reverted collector path: %v", finalMetadata)
	}
}

func TestLogLinkPublicationRetainsSourcesWhenDisabledOrUnplannable(t *testing.T) {
	submit := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name                       string
		rename, managed, knownTime bool
		path                       string
	}{
		{"disabled", false, true, true, "/logs/output.txt"},
		{"unmanaged", true, false, true, "/logs/output.txt"},
		{"missing submit time", true, true, false, "/logs/output.txt"},
		{"outside roots", true, true, true, "/elsewhere/output.txt"},
		{"relative", true, true, true, "output.txt"},
		{"APS name", true, true, true, "/logs/train_42_date_26-11-17_time_12-00-00.log"},
	} {
		t.Run(test.name, func(t *testing.T) {
			job := logLinkJob("42", 0, "", test.managed, []string{test.path})
			job.JobName = "train"
			if test.knownTime {
				job.SubmitTime = &submit
			}
			controller := Controller{LogLinkNaming: &loglinks.Config{
				RenameEnabled: test.rename, Destination: "/collector", SourceRoots: []string{"/logs"},
			}}
			got := controller.withLogLinkPaths(job)
			if !reflect.DeepEqual(got.SchedulerMetadata["slurm_log_file_paths"], []string{test.path}) {
				t.Fatalf("source path changed: %v", got.SchedulerMetadata)
			}
		})
	}
}

func TestLogLinkPublicationSeparatesJobAndSourceIdentity(t *testing.T) {
	submit := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	controller := Controller{LogLinkNaming: &loglinks.Config{
		RenameEnabled: true, Destination: "/collector", SourceRoots: []string{"/logs"},
	}}
	first := logLinkJob("42", 0, "", true, []string{"/logs/a/output", "/logs/b/output", "/logs/a/output"})
	first.SubmitTime = &submit
	second := first
	second.JobID = "43"
	a := controller.withLogLinkPaths(first).SchedulerMetadata["slurm_log_file_paths"].([]string)
	b := controller.withLogLinkPaths(second).SchedulerMetadata["slurm_log_file_paths"].([]string)
	if len(a) != 2 || len(b) != 2 || a[0] == a[1] {
		t.Fatalf("colliding or duplicate paths: %v / %v", a, b)
	}
	for _, path := range a {
		if slices.Contains(b, path) {
			t.Fatalf("jobs share generated link %q", path)
		}
	}
}

func TestLogLinkPublicationRespectsConfiguredPathBound(t *testing.T) {
	submit := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	job := logLinkJob("42", 0, "", true, []string{"/logs/output.txt"})
	job.SubmitTime = &submit
	controller := Controller{LogLinkNaming: &loglinks.Config{
		RenameEnabled: true, Destination: "/collector", SourceRoots: []string{"/logs"},
		MaxPathLength: 32,
	}}
	got := controller.withLogLinkPaths(job)
	if !reflect.DeepEqual(got.SchedulerMetadata["slurm_log_file_paths"], []string{"/logs/output.txt"}) {
		t.Fatalf("published a generated path the configured worker cannot create: %v", got.SchedulerMetadata)
	}
}
