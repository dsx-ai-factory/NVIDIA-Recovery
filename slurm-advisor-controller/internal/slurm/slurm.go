// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"go.uber.org/zap"
)

type Options struct {
	DryRun                       bool
	Timeout                      time.Duration
	AccountingWorkerPoolSize     int
	AccountingTimeout            time.Duration
	VersionedSqueueJSON          bool
	EnableLogMetadata            bool
	LogMetadataRefreshInterval   time.Duration
	LogMetadataEnrichmentTimeout time.Duration
	LogMetadataRetryBase         time.Duration
	LogMetadataQueryBudget       int
	LogMetadataParallelism       int
	LogMetadataObserver          LogMetadataObserver
	Logger                       *zap.Logger
	Clock                        func() time.Time
	// CommandObserver receives execution timeouts for scheduler commands.
	CommandObserver CommandObserver
}

type slurmCluster struct {
	timeout                  time.Duration
	logger                   *zap.Logger
	invalidRecoveryCaps      invalidRecoveryCapWarnings
	commandObserver          CommandObserver
	accountingWorkerPoolSize int
	accountingTimeout        time.Duration
	versionedSqueueJSON      bool
	rwCommands               commandExecutor
	roCommands               commandExecutor
	logMetadata              *logMetadataEnricher
	activeStdoutPaths        map[cluster.JobAttemptRef]string
	logMetadataInit          sync.Once
	logMetadataInitErr       error
}

// Cluster is the Slurm adapter plus its process-lifetime background services.
// Close cancels and joins optional workers started by NewCluster.
type Cluster interface {
	cluster.Cluster
	// DetectLogMetadataMode selects the enricher's process-lifetime log-path
	// query mode from the local Slurm client version before reconciliation starts.
	DetectLogMetadataMode(context.Context) error
	Close()
}

const defaultAccountingWorkerPoolSize = 1

type finalizedJobQuery struct {
	index int
	jobID string
}

type finalizedJobQueryResult struct {
	query    finalizedJobQuery
	jobs     []cluster.JobExecutionRecord
	logPaths map[cluster.JobAttemptRef]sacctAttemptLogPaths
	err      error
}

func NewCluster(opts Options) Cluster {
	return newSlurmCluster(opts)
}

func newSlurmCluster(opts Options) *slurmCluster {
	accountingWorkerPoolSize := opts.AccountingWorkerPoolSize
	if accountingWorkerPoolSize <= 0 {
		accountingWorkerPoolSize = defaultAccountingWorkerPoolSize
	}
	slurmCluster := &slurmCluster{
		timeout:                  opts.Timeout,
		logger:                   opts.Logger,
		commandObserver:          opts.CommandObserver,
		accountingWorkerPoolSize: accountingWorkerPoolSize,
		accountingTimeout:        opts.AccountingTimeout,
		versionedSqueueJSON:      opts.VersionedSqueueJSON,
		rwCommands:               executor{dryRun: opts.DryRun},
		roCommands:               executor{},
	}
	if opts.EnableLogMetadata {
		slurmCluster.logMetadata = newLogMetadataEnricher(opts)
		slurmCluster.logMetadata.start(slurmCluster)
	}
	return slurmCluster
}

var _ cluster.Cluster = (*slurmCluster)(nil)
var _ Cluster = (*slurmCluster)(nil)

// Close stops optional background workers and waits for in-flight read-only
// Slurm commands to observe cancellation.
func (s *slurmCluster) Close() {
	if s.logMetadata != nil {
		s.logMetadata.close()
	}
}

func (s *slurmCluster) ActiveJobs(ctx context.Context) ([]cluster.JobExecutionRecord, error) {
	return s.GetJobs(ctx)
}

func (s *slurmCluster) GetJobs(ctx context.Context) ([]cluster.JobExecutionRecord, error) {
	output, err := s.execute(ctx, s.roCommands, "squeue", s.squeueJSONArg())
	if err != nil {
		return nil, err
	}
	observations, err := observedJobsWithSourceFromSqueue(output.stdout)
	if err != nil {
		return nil, err
	}
	jobs := make([]cluster.JobExecutionRecord, len(observations))
	activeStdoutPaths := make(map[cluster.JobAttemptRef]string, len(observations))
	for i := range observations {
		jobs[i] = observations[i].Job
		activeStdoutPaths[jobAttemptRef(observations[i].Job)] = observations[i].Source.StdoutExpanded
	}
	s.activeStdoutPaths = activeStdoutPaths
	s.warnInvalidRecoveryCaps(jobs)
	return jobs, nil
}

// EnrichActiveJobs discovers best-effort log metadata for the admitted subset
// of the most recent ActiveJobs result.
func (s *slurmCluster) EnrichActiveJobs(
	ctx context.Context,
	jobs []cluster.JobExecutionRecord,
) []cluster.JobExecutionRecord {
	observations := make([]squeueObservation, len(jobs))
	for i := range jobs {
		observations[i] = squeueObservation{
			Job: jobs[i],
			Source: SlurmJob{
				StdoutExpanded: s.activeStdoutPaths[jobAttemptRef(jobs[i])],
			},
		}
	}
	return s.enrichActiveJobs(ctx, observations)
}

// FinalizedJobs queries each unique job ID through a bounded worker pool.
// Workers own individual requests; the coordinator joins all workers and
// assembles successful records and errors in stable order. A failed request
// does not discard other jobs. Cancellation stops execution of queued work.
func (s *slurmCluster) FinalizedJobs(ctx context.Context, records []cluster.JobAttemptRef) ([]cluster.JobExecutionRecord, error) {
	jobIDs := uniqueJobIDs(records)

	seen := map[cluster.JobAttemptRef]struct{}{}
	for _, record := range records {
		seen[record] = struct{}{}
	}

	accountingCtx := ctx
	if s.accountingTimeout > 0 {
		var cancel context.CancelFunc
		accountingCtx, cancel = context.WithTimeout(ctx, s.accountingTimeout)
		defer cancel()
	}

	workerCount := min(s.accountingWorkerPoolSize, len(jobIDs))
	queries := make(chan finalizedJobQuery, workerCount)
	results := make(chan finalizedJobQueryResult, len(jobIDs))
	var workers sync.WaitGroup
	for range workerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for query := range queries {
				result := finalizedJobQueryResult{query: query}
				if ctxErr := accountingCtx.Err(); ctxErr != nil {
					result.err = ctxErr
				} else {
					result.jobs, result.logPaths, result.err = s.queryFinalizedJob(accountingCtx, query.jobID)
				}
				if result.err != nil {
					result.err = errors.Wrapf(result.err, "query finalized Slurm job %s", query.jobID)
				}
				results <- result
			}
		}()
	}

	submitted, admissionErr := submitFinalizedJobQueries(accountingCtx, jobIDs, queries)
	close(queries)
	workers.Wait()
	close(results)

	orderedResults := make([]finalizedJobQueryResult, submitted)
	for result := range results {
		orderedResults[result.query.index] = result
	}

	var errs []error
	for _, result := range orderedResults {
		if result.err != nil {
			errs = append(errs, result.err)
		}
	}
	if admissionErr != nil {
		errs = append(errs, admissionErr)
	}
	jobs := make([]cluster.JobExecutionRecord, 0, len(records))
	nativePaths := make(map[cluster.JobAttemptRef]sacctAttemptLogPaths)
	for _, result := range orderedResults {
		if result.err != nil {
			continue
		}
		for ref, paths := range result.logPaths {
			nativePaths[ref] = paths
		}
		for _, job := range result.jobs {
			if _, ok := seen[cluster.JobAttemptRef{JobID: job.JobID, AttemptIndex: job.AttemptIndex}]; ok {
				jobs = append(jobs, job)
			}
		}
	}
	// Normalize by stable identity so callers do not observe sacct output order or worker timing.
	slices.SortStableFunc(jobs, func(left, right cluster.JobExecutionRecord) int {
		if left.JobID != right.JobID {
			return strings.Compare(left.JobID, right.JobID)
		}
		if left.AttemptIndex < right.AttemptIndex {
			return -1
		}
		if left.AttemptIndex > right.AttemptIndex {
			return 1
		}
		return 0
	})

	s.warnInvalidRecoveryCaps(jobs)
	return s.enrichFinalizedJobs(ctx, jobs, nativePaths), errors.Join(errs...)
}

func submitFinalizedJobQueries(
	ctx context.Context,
	jobIDs []string,
	queries chan<- finalizedJobQuery,
) (int, error) {
	submitted := 0
	for index, jobID := range jobIDs {
		select {
		case queries <- finalizedJobQuery{index: index, jobID: jobID}:
			submitted++
		case <-ctx.Done():
			return submitted, errors.Wrap(ctx.Err(), "submit targeted accounting queries")
		}
	}
	return submitted, nil
}

func (s *slurmCluster) queryFinalizedJob(
	ctx context.Context,
	jobID string,
) ([]cluster.JobExecutionRecord, map[cluster.JobAttemptRef]sacctAttemptLogPaths, error) {
	args := []string{"-Xj", jobID, "--duplicates", "--json"}
	output, err := s.execute(ctx, s.roCommands, "sacct", args...)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, nil, errors.Join(ctxErr, err)
		}
		return nil, nil, err
	}
	jobs, err := observedJobsFromSacct(output.stdout)
	s.logSacctOutput(jobID, "sacct_status", args, output, err)
	if err != nil {
		return nil, nil, err
	}
	var paths map[cluster.JobAttemptRef]sacctAttemptLogPaths
	if s.logMetadata != nil && s.logMetadata.legacyMode.Load() {
		// Reuse expanded parent paths already returned by the status query.
		paths, _ = logPathsFromSacct(output.stdout)
	}
	return jobs, paths, nil
}

func (s *slurmCluster) JobKill(ctx context.Context, jobID string) error {
	_, err := s.execute(ctx, s.rwCommands, "scancel", jobID)
	return err
}

func (s *slurmCluster) JobHold(ctx context.Context, jobID string) error {
	_, err := s.execute(ctx, s.rwCommands, "scontrol", "hold", jobID)
	return err
}

func (s *slurmCluster) JobRelease(ctx context.Context, jobID string) error {
	_, err := s.execute(ctx, s.rwCommands, "scontrol", "release", jobID)
	return err
}

func (s *slurmCluster) JobRequeueHold(ctx context.Context, jobID string) error {
	_, err := s.execute(ctx, s.rwCommands, "scontrol", "requeuehold", jobID)
	return classifyRequeueError(err)
}

func (s *slurmCluster) JobUpdateEligibleNow(ctx context.Context, jobID string) error {
	_, err := s.execute(ctx, s.rwCommands, "scontrol", "update", "JobID="+jobID, "EligibleTime=Now")
	return err
}

func (s *slurmCluster) JobSignal(ctx context.Context, jobID, signal string) error {
	steps, err := s.findJobSteps(ctx, jobID)
	if err != nil {
		return err
	}

	var pick string
	for _, step := range steps {
		if step != "batch" && step != "extern" {
			pick = step
			break
		}
	}
	if pick == "" {
		return errors.Newf("cannot find required job step to send signal to (found %v)", steps)
	}

	_, err = s.execute(ctx, s.rwCommands, "scancel", "--ctld", fmt.Sprintf("--signal=%s", signal), fmt.Sprintf("%s.%s", jobID, pick))
	return err
}

func (s *slurmCluster) findJobSteps(ctx context.Context, jobID string) ([]string, error) {
	output, err := s.execute(ctx, s.roCommands, "squeue", "-j", jobID, "-s", "-o", "%i", "-h")
	if err != nil {
		return nil, err
	}

	lines := strings.Split(strings.TrimSpace(string(output.stdout)), "\n")
	steps := []string{}
	for _, line := range lines {
		if line == "" {
			continue
		}
		parts := strings.Split(line, ".")
		if len(parts) != 2 {
			return nil, errors.Newf("unexpected output format from squeue: %s", line)
		}
		if parts[0] != jobID {
			return nil, errors.Newf("unexpected job ID in output: %s", parts[0])
		}
		steps = append(steps, parts[1])
	}

	return steps, nil
}

func (s *slurmCluster) JobNodeUpdate(ctx context.Context, jobID string, excludeList, includeList []string, nodeCount int) error {
	if len(excludeList) == 0 && len(includeList) == 0 && nodeCount == 0 {
		return cluster.ErrEmptyNodeUpdate
	}
	if nodeCount < 0 {
		return errors.Wrapf(cluster.ErrInvalidNodeCount, "node count %d", nodeCount)
	}
	if overlap := overlappingNodes(excludeList, includeList); len(overlap) > 0 {
		return errors.Wrapf(cluster.ErrNodeUpdateOverlap, "overlapping nodes %s", strings.Join(overlap, ","))
	}

	args := []string{"update", "JobID=" + jobID}
	if nodeCount > 0 {
		args = append(args, fmt.Sprintf("NumNodes=%d", nodeCount))
	}
	if len(includeList) > 0 {
		args = append(args, "ReqNodeList="+strings.Join(normalizeNodes(includeList), ","))
	}
	if len(excludeList) > 0 {
		args = append(args, "ExcNodeList="+strings.Join(normalizeNodes(excludeList), ","))
	}

	_, err := s.execute(ctx, s.rwCommands, "scontrol", args...)
	return err
}

func (s *slurmCluster) squeueJSONArg() string {
	if s.versionedSqueueJSON {
		return "--json=v0.0.40"
	}
	return "--json"
}

func uniqueJobIDs(records []cluster.JobAttemptRef) []string {
	out := []string{}
	seen := map[string]struct{}{}
	for _, record := range records {
		if _, ok := seen[record.JobID]; ok {
			continue
		}
		seen[record.JobID] = struct{}{}
		out = append(out, record.JobID)
	}
	// Keep query admission and error aggregation deterministic for every caller.
	slices.Sort(out)
	return out
}

func overlappingNodes(left, right []string) []string {
	rightSet := map[string]struct{}{}
	for _, node := range normalizeNodes(right) {
		rightSet[node] = struct{}{}
	}

	var overlap []string
	for _, node := range normalizeNodes(left) {
		if _, ok := rightSet[node]; ok {
			overlap = append(overlap, node)
		}
	}
	return overlap
}

func normalizeNodes(nodes []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if node == "" {
			continue
		}
		if _, ok := seen[node]; ok {
			continue
		}
		seen[node] = struct{}{}
		out = append(out, node)
	}
	slices.Sort(out)
	return out
}
