// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"go.uber.org/zap"
)

const (
	defaultLogMetadataRefreshInterval = 5 * time.Minute
	defaultLogMetadataRetryBase       = 15 * time.Second
	// Keep the command budget as a high safety ceiling; parallelism and the
	// enrichment deadline are expected to be the effective runtime limits.
	defaultLogMetadataQueryBudget       = 500
	defaultLogMetadataParallelism       = 8
	defaultLogMetadataEnrichmentTimeout = 2 * time.Second
	defaultLogMetadataCacheMaxPathBytes = 64 << 20
	defaultLogMetadataCacheMaxEntries   = 65_536
	defaultLogMetadataQueueCapacity     = 1_024
	logMetadataImmediateAttempts        = 2
	slurmLogFilePathsMetadataKey        = "slurm_log_file_paths"
	maxSlurmLogFilePaths                = 256
	maxSlurmLogFilePathBytes            = 4096
)

var errLogMetadataQueryBudgetExhausted = errors.New("log metadata query budget exhausted")

// LogFilePaths returns a copy of the Slurm-owned log path metadata for a job.
// Callers cannot mutate the scheduler metadata through the returned slice.
func LogFilePaths(job cluster.JobExecutionRecord) []string {
	paths, ok := job.SchedulerMetadata[slurmLogFilePathsMetadataKey].([]string)
	if !ok {
		return nil
	}

	return slices.Clone(paths)
}

type logMetadataEnricher struct {
	// The adapter sets the query mode during startup. JSON is the default;
	// atomic access keeps startup selection safe for the background workers.
	legacyMode atomic.Bool

	// These values are fixed at construction and bound refresh frequency,
	// retries, command concurrency, and cache retention.
	refreshInterval   time.Duration
	retryBase         time.Duration
	queryBudget       int
	parallelism       int
	enrichmentTimeout time.Duration
	cacheTTL          time.Duration
	cacheMaxPathBytes int
	cacheMaxEntries   int

	// External collaborators are immutable after construction. The clock is
	// injected so retry, refresh, and eviction behavior can be tested reliably.
	logger   *zap.Logger
	clock    func() time.Time
	observer LogMetadataObserver

	// mu keeps observation, admission, worker completion, eviction, and
	// publication atomic for an exact attempt. Slurm commands and observer
	// callbacks always run after releasing it.
	mu              sync.Mutex
	attempts        map[cluster.JobAttemptRef]*attemptLogMetadata
	nextGeneration  uint64
	workersInFlight int
	accepting       bool

	// The bounded queue and fixed worker set own asynchronous command execution.
	// Canceling workerContext stops command work; closeOnce makes shutdown safe
	// when more than one owner attempts to close the cluster.
	workQueue     chan logMetadataWork
	workerContext context.Context
	cancelWorkers context.CancelFunc
	workers       sync.WaitGroup
	closeOnce     sync.Once
}

type attemptLogMetadata struct {
	observed       bool
	state          string
	lastObservedAt time.Time
	generation     uint64
	queued         bool
	queuedAt       time.Time
	inFlight       bool
	parent         logMetadataSource
	steps          logMetadataSource
}

type logMetadataSource struct {
	paths         []string
	initialized   bool
	refreshedAt   time.Time
	lastAttemptAt time.Time
	nextRetryAt   time.Time
	failures      uint
}

type sacctAttemptLogPaths struct {
	parent []string
	steps  []string

	missingParent bool
	missingSteps  bool

	invalidParent int
	invalidSteps  int
}

type logRefreshCandidate struct {
	index       int
	ref         cluster.JobAttemptRef
	generation  uint64
	parentDue   bool
	stepsDue    bool
	lastAttempt time.Time
}

type logMetadataWork struct {
	ref        cluster.JobAttemptRef
	generation uint64
	parentDue  bool
	stepsDue   bool
	queuedAt   time.Time
}

type finalizedRefreshGroup struct {
	jobID       string
	candidates  []logRefreshCandidate
	lastAttempt time.Time
	nativePaths map[cluster.JobAttemptRef]sacctAttemptLogPaths
}

// logMetadataSharedBudget is the synchronized pool left after each selected
// candidate receives one reserved command. Concurrent candidates draw from it
// for immediate retries and the optional scontrol fallback.
type logMetadataSharedBudget struct {
	mu        sync.Mutex
	remaining int
}

// logMetadataCandidateBudget gives one candidate exclusive access to its
// reserved command before falling back to the shared per-tick pool.
type logMetadataCandidateBudget struct {
	reserved int
	shared   *logMetadataSharedBudget
}

func newLogMetadataEnricher(opts Options) *logMetadataEnricher {
	refreshInterval := opts.LogMetadataRefreshInterval
	if refreshInterval <= 0 {
		refreshInterval = defaultLogMetadataRefreshInterval
	}
	retryBase := opts.LogMetadataRetryBase
	if retryBase <= 0 {
		retryBase = defaultLogMetadataRetryBase
	}
	if retryBase > refreshInterval {
		retryBase = refreshInterval
	}
	enrichmentTimeout := opts.LogMetadataEnrichmentTimeout
	if enrichmentTimeout <= 0 {
		enrichmentTimeout = defaultLogMetadataEnrichmentTimeout
	}
	queryBudget := opts.LogMetadataQueryBudget
	if queryBudget <= 0 {
		queryBudget = defaultLogMetadataQueryBudget
	}
	parallelism := opts.LogMetadataParallelism
	if parallelism <= 0 {
		parallelism = defaultLogMetadataParallelism
	}
	logger := opts.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	clock := opts.Clock
	if clock == nil {
		clock = time.Now
	}
	observer := opts.LogMetadataObserver
	if observer == nil {
		observer = NopLogMetadataObserver{}
	}
	workerContext, cancelWorkers := context.WithCancel(context.Background())

	return &logMetadataEnricher{
		refreshInterval:   refreshInterval,
		retryBase:         retryBase,
		queryBudget:       queryBudget,
		parallelism:       parallelism,
		enrichmentTimeout: enrichmentTimeout,
		cacheTTL:          2 * refreshInterval,
		cacheMaxPathBytes: defaultLogMetadataCacheMaxPathBytes,
		cacheMaxEntries:   defaultLogMetadataCacheMaxEntries,
		logger:            logger,
		clock:             clock,
		attempts:          make(map[cluster.JobAttemptRef]*attemptLogMetadata),
		workQueue:         make(chan logMetadataWork, defaultLogMetadataQueueCapacity),
		workerContext:     workerContext,
		cancelWorkers:     cancelWorkers,
		accepting:         true,
		observer:          observer,
	}
}

func (s *slurmCluster) enrichActiveJobs(
	ctx context.Context,
	observations []squeueObservation,
) []cluster.JobExecutionRecord {
	jobs := make([]cluster.JobExecutionRecord, len(observations))
	for i := range observations {
		jobs[i] = observations[i].Job
	}
	if s.logMetadata == nil {
		return jobs
	}

	enricher := s.logMetadata
	currentTime := enricher.clock()
	candidates := make([]logRefreshCandidate, 0, len(observations))
	candidateRefs := make(map[cluster.JobAttemptRef]struct{}, len(observations))
	cacheAccesses := make([]struct {
		source LogMetadataCacheSource
		result LogMetadataCacheResult
	}, 0, 2*len(observations))
	workEvents := make([]LogMetadataWorkEvent, 0, len(observations))
	invalidStructuredParents := make([]cluster.JobAttemptRef, 0)

	enricher.mu.Lock()
	for i := range observations {
		job := &jobs[i]
		ref := jobAttemptRef(*job)
		entry, stateChanged := enricher.observeLocked(ref, job.RawState, currentTime)
		parentWasDue := entry.parent.dueParent(
			currentTime,
			stateChanged,
			enricher.refreshInterval,
		)
		parentCacheResult := cacheResult(entry.parent, parentWasDue)
		stepsDue := entry.steps.due(currentTime, stateChanged, enricher.refreshInterval)
		stepsCacheResult := cacheResult(entry.steps, stepsDue)

		hasStructuredParent := false
		if observations[i].Source.StdoutExpanded != "" {
			paths, invalid := normalizeDiscoveredPaths([]string{observations[i].Source.StdoutExpanded})
			if invalid == 0 {
				entry.parent.succeed(paths, currentTime)
				hasStructuredParent = true
			} else {
				invalidStructuredParents = append(invalidStructuredParents, ref)
			}
		}

		parentDue := !hasStructuredParent && entry.parent.dueParent(
			currentTime,
			stateChanged,
			enricher.refreshInterval,
		)
		cacheAccesses = append(cacheAccesses,
			struct {
				source LogMetadataCacheSource
				result LogMetadataCacheResult
			}{LogMetadataCacheSourceParent, parentCacheResult},
			struct {
				source LogMetadataCacheSource
				result LogMetadataCacheResult
			}{LogMetadataCacheSourceSteps, stepsCacheResult},
		)
		if parentDue || stepsDue {
			workEvents = append(workEvents, LogMetadataWorkOffered)
			if _, exists := candidateRefs[ref]; exists {
				workEvents = append(workEvents, LogMetadataWorkDeduplicated)
				continue
			}
			if entry.queued || entry.inFlight {
				workEvents = append(workEvents, LogMetadataWorkDeduplicated)
				continue
			}
			candidateRefs[ref] = struct{}{}
			candidates = append(candidates, logRefreshCandidate{
				index:       i,
				ref:         ref,
				generation:  entry.generation,
				parentDue:   parentDue,
				stepsDue:    stepsDue,
				lastAttempt: oldestAttempt(entry, parentDue, stepsDue),
			})
		}
	}

	sortRefreshCandidates(candidates)
	if ctx.Err() != nil || !enricher.accepting {
		for range candidates {
			workEvents = append(workEvents, LogMetadataWorkCanceled)
		}
	} else {
		for _, candidate := range candidates {
			entry, ok := enricher.attempts[candidate.ref]
			if !ok || entry.generation != candidate.generation || entry.queued || entry.inFlight {
				workEvents = append(workEvents, LogMetadataWorkDeduplicated)
				continue
			}
			work := logMetadataWork{
				ref:        candidate.ref,
				generation: candidate.generation,
				parentDue:  candidate.parentDue,
				stepsDue:   candidate.stepsDue,
				queuedAt:   currentTime,
			}
			entry.queued = true
			entry.queuedAt = currentTime
			select {
			case enricher.workQueue <- work:
				workEvents = append(workEvents, LogMetadataWorkAdmitted)
			default:
				entry.queued = false
				entry.queuedAt = time.Time{}
				workEvents = append(workEvents, LogMetadataWorkQueueFull)
			}
		}
	}

	evictions := enricher.enforceLimitsLocked(currentTime)
	publishedPaths := 0
	publishedAttempts := 0
	for i := range jobs {
		pathCount := enricher.publishLocked(&jobs[i])
		publishedPaths += pathCount
		if pathCount > 0 {
			publishedAttempts++
		}
	}
	cacheObservation := enricher.cacheObservationLocked(currentTime)
	enricher.mu.Unlock()

	for _, ref := range invalidStructuredParents {
		enricher.logInvalid(ref, "squeue_parent", 1)
	}
	for _, access := range cacheAccesses {
		enricher.observer.ObserveLogMetadataCacheAccess(access.source, access.result)
	}
	for _, event := range workEvents {
		enricher.observer.ObserveLogMetadataWork(event)
	}
	enricher.observeEvictions(evictions)
	if publishedPaths > 0 {
		enricher.observer.ObserveLogMetadataPublished(publishedAttempts, publishedPaths)
	}
	enricher.observer.ObserveLogMetadataCache(cacheObservation)
	return jobs
}

func (e *logMetadataEnricher) start(slurmCluster *slurmCluster) {
	for range e.parallelism {
		e.workers.Add(1)
		go func() {
			defer e.workers.Done()
			e.runWorker(slurmCluster)
		}()
	}
	e.mu.Lock()
	observation := e.cacheObservationLocked(e.clock())
	e.mu.Unlock()
	e.observer.ObserveLogMetadataCache(observation)
}

func (e *logMetadataEnricher) close() {
	e.closeOnce.Do(func() {
		e.mu.Lock()
		e.accepting = false
		e.mu.Unlock()
		e.cancelWorkers()
		e.workers.Wait()

		canceled := 0
	drainQueue:
		for {
			select {
			case <-e.workQueue:
				canceled++
			default:
				break drainQueue
			}
		}

		e.mu.Lock()
		for _, entry := range e.attempts {
			entry.queued = false
			entry.queuedAt = time.Time{}
			entry.inFlight = false
		}
		observation := e.cacheObservationLocked(e.clock())
		e.mu.Unlock()
		for range canceled {
			e.observer.ObserveLogMetadataWork(LogMetadataWorkCanceled)
		}
		e.observer.ObserveLogMetadataCache(observation)
	})
}

func (e *logMetadataEnricher) runWorker(slurmCluster *slurmCluster) {
	for {
		select {
		case <-e.workerContext.Done():
			return
		case work := <-e.workQueue:
			if e.workerContext.Err() != nil {
				return
			}
			if !e.claimWork(work) {
				e.observer.ObserveLogMetadataWork(LogMetadataWorkStale)
				continue
			}
			e.observer.ObserveLogMetadataWork(LogMetadataWorkStarted)
			itemCtx, cancel := context.WithTimeout(e.workerContext, e.enrichmentTimeout)
			failed, stale := slurmCluster.runActiveLogMetadataWork(itemCtx, work)
			ctxErr := itemCtx.Err()
			cancel()
			e.finishWork(work)

			switch {
			case stale:
				e.observer.ObserveLogMetadataWork(LogMetadataWorkStale)
			case errors.Is(ctxErr, context.DeadlineExceeded):
				e.observer.ObserveLogMetadataWork(LogMetadataWorkTimedOut)
			case errors.Is(ctxErr, context.Canceled):
				e.observer.ObserveLogMetadataWork(LogMetadataWorkCanceled)
			case failed:
				e.observer.ObserveLogMetadataWork(LogMetadataWorkFailed)
			default:
				e.observer.ObserveLogMetadataWork(LogMetadataWorkCompleted)
			}
		}
	}
}

func (e *logMetadataEnricher) claimWork(work logMetadataWork) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	entry, ok := e.attempts[work.ref]
	if !ok || entry.generation != work.generation || !entry.queued {
		return false
	}
	entry.queued = false
	entry.queuedAt = time.Time{}
	entry.inFlight = true
	e.workersInFlight++
	return true
}

func (e *logMetadataEnricher) finishWork(work logMetadataWork) {
	currentTime := e.clock()
	e.mu.Lock()
	if e.workersInFlight > 0 {
		e.workersInFlight--
	}
	if entry, ok := e.attempts[work.ref]; ok && entry.generation == work.generation {
		entry.inFlight = false
	}
	evictions := e.enforceLimitsLocked(currentTime)
	observation := e.cacheObservationLocked(currentTime)
	e.mu.Unlock()
	e.observeEvictions(evictions)
	e.observer.ObserveLogMetadataCache(observation)
}

func (s *slurmCluster) runActiveLogMetadataWork(
	ctx context.Context,
	work logMetadataWork,
) (failed bool, stale bool) {
	if work.stepsDue {
		paths, err := s.queryActiveLogPaths(ctx, work.ref)
		if err != nil {
			s.logMetadata.logFailure(work.ref, "sacct_steps", err)
			failed = true
		}
		// Both modes can return partial paths. Completion
		// checks the generation before retaining those results for a retry.
		if !s.logMetadata.applyActiveSacct(work, paths) {
			return failed, true
		}
	}

	shouldWorkOnParent := work.parentDue && s.logMetadata.parentFallbackDue(work)
	if !shouldWorkOnParent {
		return failed, false
	}

	output, err := s.executeActiveLogMetadataCommand(
		ctx,
		LogMetadataCommandScontrol,
		"scontrol",
		"show",
		"job",
		"-o",
		work.ref.JobID,
	)
	if err != nil {
		if !s.logMetadata.failActiveSource(work, LogMetadataCacheSourceParent) {
			return true, true
		}
		s.logMetadata.logFailure(work.ref, "scontrol_parent", err)
		return true, false
	}
	stdout, attemptIndex, stdoutFound, attemptFound := logMetadataFromScontrol(output.stdout)
	if !attemptFound || attemptIndex != work.ref.AttemptIndex {
		return failed, true
	}
	if !stdoutFound {
		err := errors.New("scontrol job output does not contain StdOut")
		if !s.logMetadata.failActiveSource(work, LogMetadataCacheSourceParent) {
			return true, true
		}
		s.logMetadata.logFailure(work.ref, "scontrol_parent", err)
		return true, false
	}

	var raw []string
	if stdout != "" {
		raw = []string{stdout}
	}
	paths, invalid := normalizeDiscoveredPaths(raw)
	if invalid > 0 {
		if !s.logMetadata.failActiveSource(work, LogMetadataCacheSourceParent) {
			return true, true
		}
		s.logMetadata.logInvalid(work.ref, "scontrol_parent", invalid)
		return true, false
	}
	if !s.logMetadata.succeedActiveParent(work, paths) {
		return failed, true
	}
	return failed, false
}

// queryActiveLogPaths uses the startup-selected mode within one work item's
// deadline. Both modes select the observed attempt's accounting paths; active
// parent discovery remains on squeue/scontrol.
func (s *slurmCluster) queryActiveLogPaths(
	ctx context.Context,
	ref cluster.JobAttemptRef,
) (sacctAttemptLogPaths, error) {
	runner := func(ctx context.Context, name string, args ...string) (commandOutput, error) {
		return s.executeActiveLogMetadataCommand(ctx, LogMetadataCommandSacct, name, args...)
	}

	var discovered map[cluster.JobAttemptRef]sacctAttemptLogPaths
	var err error
	if s.logMetadata.legacyMode.Load() {
		discovered, err = queryLegacyLogPaths(ctx, ref.JobID,
			[]logRefreshCandidate{{ref: ref, stepsDue: true}}, runner)
	} else {
		discovered, err = s.queryJSONLogPaths(ctx, ref.JobID, "sacct_steps", runner)
	}

	// A legacy parsing error can accompany valid paths for this attempt.
	paths, found := discovered[ref]
	if !found {
		if err == nil {
			err = errors.New("sacct log metadata is missing the active attempt")
		}
		return sacctAttemptLogPaths{missingSteps: true}, err
	}
	return paths, err
}

func (s *slurmCluster) executeActiveLogMetadataCommand(
	ctx context.Context,
	command LogMetadataCommand,
	name string,
	args ...string,
) (commandOutput, error) {
	var lastErr error
	for range logMetadataImmediateAttempts {
		startedAt := time.Now()
		output, err := s.execute(ctx, s.roCommands, name, args...)
		s.logMetadata.observer.ObserveLogMetadataCommand(
			command,
			classifyLogMetadataCommandOutcome(ctx, err),
			time.Since(startedAt),
		)
		if err == nil {
			return output, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			break
		}
	}
	return commandOutput{}, lastErr
}

func (s *slurmCluster) enrichFinalizedJobs(
	ctx context.Context,
	jobs []cluster.JobExecutionRecord,
	nativePaths map[cluster.JobAttemptRef]sacctAttemptLogPaths,
) []cluster.JobExecutionRecord {
	if s.logMetadata == nil || len(jobs) == 0 {
		return jobs
	}

	enricher := s.logMetadata
	currentTime := enricher.clock()
	groupsByJobID := make(map[string]*finalizedRefreshGroup)
	groupOrder := make([]string, 0)
	enricher.mu.Lock()
	for i := range jobs {
		ref := jobAttemptRef(jobs[i])
		entry, stateChanged := enricher.observeLocked(ref, jobs[i].RawState, currentTime)
		if entry.queued || entry.inFlight {
			entry.generation = enricher.newGenerationLocked()
			entry.queued = false
			entry.queuedAt = time.Time{}
			entry.inFlight = false
		}
		parentDue := entry.parent.dueParent(currentTime, stateChanged, enricher.refreshInterval)
		stepsDue := entry.steps.due(currentTime, stateChanged, enricher.refreshInterval)
		if !parentDue && !stepsDue {
			continue
		}

		group, ok := groupsByJobID[jobs[i].JobID]
		if !ok {
			group = &finalizedRefreshGroup{jobID: jobs[i].JobID, nativePaths: nativePaths}
			groupsByJobID[jobs[i].JobID] = group
			groupOrder = append(groupOrder, jobs[i].JobID)
		}
		candidate := logRefreshCandidate{
			index:       i,
			ref:         ref,
			generation:  entry.generation,
			parentDue:   parentDue,
			stepsDue:    stepsDue,
			lastAttempt: oldestAttempt(entry, parentDue, stepsDue),
		}
		group.candidates = append(group.candidates, candidate)
		if len(group.candidates) == 1 {
			group.lastAttempt = candidate.lastAttempt
		} else {
			group.lastAttempt = earlierRefreshTime(group.lastAttempt, candidate.lastAttempt)
		}
	}
	evictions := enricher.enforceLimitsLocked(currentTime)
	cacheObservation := enricher.cacheObservationLocked(currentTime)
	enricher.mu.Unlock()
	enricher.observeEvictions(evictions)
	enricher.observer.ObserveLogMetadataCache(cacheObservation)

	groups := make([]finalizedRefreshGroup, 0, len(groupOrder))
	for _, jobID := range groupOrder {
		groups = append(groups, *groupsByJobID[jobID])
	}
	slices.SortStableFunc(groups, func(left, right finalizedRefreshGroup) int {
		return compareRefreshOrder(left.lastAttempt, left.jobID, right.lastAttempt, right.jobID)
	})

	enrichmentCtx, cancel := enricher.context(ctx)
	defer cancel()
	// Finalized log metadata is supplemental and fail-open. Each group owns all
	// attempt entries for one job ID; failures are logged and recorded for retry
	// without discarding valid finalized job state.
	runLogMetadataRefreshBatch(
		enrichmentCtx,
		groups,
		enricher.queryBudget,
		enricher.parallelism,
		func(ctx context.Context, group finalizedRefreshGroup, budget *logMetadataCandidateBudget) {
			s.refreshFinalizedLogMetadataGroup(ctx, group, currentTime, budget)
		},
	)

	enricher.mu.Lock()
	publishedPaths := 0
	publishedAttempts := 0
	for i := range jobs {
		pathCount := enricher.publishLocked(&jobs[i])
		publishedPaths += pathCount
		if pathCount > 0 {
			publishedAttempts++
		}
	}
	evictions = enricher.enforceLimitsLocked(currentTime)
	cacheObservation = enricher.cacheObservationLocked(currentTime)
	enricher.mu.Unlock()
	enricher.observeEvictions(evictions)
	if publishedPaths > 0 {
		enricher.observer.ObserveLogMetadataPublished(publishedAttempts, publishedPaths)
	}
	enricher.observer.ObserveLogMetadataCache(cacheObservation)
	return jobs
}

// runLogMetadataRefreshBatch executes independent, preordered refresh units
// under one command budget and concurrency limit. At most queryBudget units are
// admitted. Each receives one reserved command so a later unit cannot lose its
// first attempt to another unit's retry; additional commands compete for the
// synchronized shared remainder. Cancellation stops admission, and the function
// waits for started callbacks so their cache writes finish before publication or
// pruning. Workers recheck cancellation before invoking callbacks so queued
// work cannot turn a batch deadline into per-item failure and backoff updates.
func runLogMetadataRefreshBatch[T any](
	ctx context.Context,
	items []T,
	queryBudget int,
	parallelism int,
	refresh func(context.Context, T, *logMetadataCandidateBudget),
) {
	if len(items) == 0 || queryBudget <= 0 || ctx.Err() != nil {
		return
	}

	itemCount := min(len(items), queryBudget)
	items = items[:itemCount]
	sharedBudget := &logMetadataSharedBudget{remaining: queryBudget - itemCount}
	semaphore := make(chan struct{}, min(parallelism, itemCount))
	var waitGroup sync.WaitGroup
	for _, item := range items {
		select {
		case semaphore <- struct{}{}:
		case <-ctx.Done():
			waitGroup.Wait()
			return
		}

		waitGroup.Add(1)
		go func(item T) {
			defer waitGroup.Done()
			defer func() { <-semaphore }()
			if ctx.Err() != nil {
				return
			}

			budget := &logMetadataCandidateBudget{reserved: 1, shared: sharedBudget}
			refresh(ctx, item, budget)
		}(item)
	}
	waitGroup.Wait()
}

// refreshFinalizedLogMetadataGroup refreshes every due attempt candidate for
// one job ID. sacct --duplicates returns accounting records for all attempts of
// that ID in one response, using the startup-selected text or JSON mode.
func (s *slurmCluster) refreshFinalizedLogMetadataGroup(
	ctx context.Context,
	group finalizedRefreshGroup,
	currentTime time.Time,
	budget *logMetadataCandidateBudget,
) {
	enricher := s.logMetadata
	runner := func(ctx context.Context, name string, args ...string) (commandOutput, error) {
		return s.executeLogMetadataCommand(ctx, budget, name, args...)
	}
	discovered, err := s.queryFinalizedLogPaths(ctx, group, runner)
	for _, candidate := range group.candidates {
		paths, found := discovered[candidate.ref]
		if err != nil {
			enricher.logFailure(candidate.ref, "sacct_finalized", err)
			if !found {
				enricher.failCandidate(candidate, currentTime)
				continue
			}
		}
		enricher.updateCandidate(candidate, paths, currentTime)
	}
}

// queryFinalizedLogPaths uses text for legacy clients and native JSON for newer
// or unknown versions. Legacy mode reuses parent paths from status accounting.
func (s *slurmCluster) queryFinalizedLogPaths(
	ctx context.Context,
	group finalizedRefreshGroup,
	runner logMetadataCommandRunner,
) (map[cluster.JobAttemptRef]sacctAttemptLogPaths, error) {
	if s.logMetadata.legacyMode.Load() {
		candidates := slices.Clone(group.candidates)
		for i := range candidates {
			native := group.nativePaths[candidates[i].ref]
			if len(native.parent) > 0 && !native.missingParent {
				// Status JSON already supplied a complete parent path. Skip sbatch
				// parsing on the copy while preserving the caller's parent-cache update plan.
				candidates[i].parentDue = false
			}
		}
		results, err := queryLegacyLogPaths(ctx, group.jobID, candidates, runner)
		if results == nil {
			results = make(map[cluster.JobAttemptRef]sacctAttemptLogPaths)
		}
		// The text parser creates a new result map. Add native parent paths here
		// even when text discovery fails; step completeness stays independent.
		for _, candidate := range group.candidates {
			native := group.nativePaths[candidate.ref]
			if len(native.parent) == 0 {
				continue
			}
			paths, found := results[candidate.ref]
			if !found {
				paths.missingParent, paths.missingSteps = candidate.parentDue, candidate.stepsDue
			}
			paths.parent = unionLogPaths(paths.parent, native.parent)
			if !native.missingParent {
				paths.missingParent = false
			}
			results[candidate.ref] = paths
		}
		return results, err
	}
	return s.queryJSONLogPaths(ctx, group.jobID, "sacct_finalized", runner)
}

// executeLogMetadataCommand consumes budget per actual command attempt. Budget
// exhaustion before the first attempt returns a sentinel so callers can skip
// cache failure/backoff updates for an RPC that never ran.
func (s *slurmCluster) executeLogMetadataCommand(
	ctx context.Context,
	budget *logMetadataCandidateBudget,
	name string,
	args ...string,
) (commandOutput, error) {
	var lastErr error
	for attempt := 0; attempt < logMetadataImmediateAttempts && budget.take(); attempt++ {
		output, err := s.execute(ctx, s.roCommands, name, args...)
		if err == nil {
			return output, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			break
		}
	}
	if lastErr == nil {
		lastErr = errLogMetadataQueryBudgetExhausted
	}
	return commandOutput{}, lastErr
}

func (budget *logMetadataSharedBudget) take() bool {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if budget.remaining <= 0 {
		return false
	}
	budget.remaining--
	return true
}

func (budget *logMetadataCandidateBudget) take() bool {
	if budget.reserved > 0 {
		budget.reserved--
		return true
	}
	return budget.shared.take()
}

func (e *logMetadataEnricher) observeLocked(
	ref cluster.JobAttemptRef,
	state string,
	currentTime time.Time,
) (*attemptLogMetadata, bool) {
	entry, ok := e.attempts[ref]
	if !ok {
		entry = &attemptLogMetadata{generation: e.newGenerationLocked()}
		e.attempts[ref] = entry
	}
	stateChanged := entry.observed && entry.state != state
	entry.observed = true
	entry.state = state
	entry.lastObservedAt = currentTime
	return entry, stateChanged
}

// observe is retained for focused cache tests. Production callers hold the
// lock across observation, admission, and publication through observeLocked.
func (e *logMetadataEnricher) observe(
	ref cluster.JobAttemptRef,
	state string,
	currentTime time.Time,
) (*attemptLogMetadata, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.observeLocked(ref, state, currentTime)
}

func (e *logMetadataEnricher) newGenerationLocked() uint64 {
	e.nextGeneration++
	return e.nextGeneration
}

func (e *logMetadataEnricher) context(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, e.enrichmentTimeout)
}

func (e *logMetadataEnricher) enforceLimitsLocked(
	currentTime time.Time,
) map[LogMetadataEvictionReason]int {
	evictions := make(map[LogMetadataEvictionReason]int)
	if e.cacheTTL > 0 {
		staleBefore := currentTime.Add(-e.cacheTTL)
		for ref, entry := range e.attempts {
			if entry.lastObservedAt.Before(staleBefore) {
				delete(e.attempts, ref)
				evictions[LogMetadataEvictionTTL]++
			}
		}
	}

	pathBytes := e.cachePathBytesLocked()
	needsEntryEviction := e.cacheMaxEntries > 0 && len(e.attempts) > e.cacheMaxEntries
	needsPathEviction := e.cacheMaxPathBytes > 0 && pathBytes > e.cacheMaxPathBytes
	if !needsEntryEviction && !needsPathEviction {
		return evictions
	}

	refs := e.sortedAttemptRefsLocked()
	if needsEntryEviction {
		removeCount := len(refs) - e.cacheMaxEntries
		for _, ref := range refs[:removeCount] {
			delete(e.attempts, ref)
			evictions[LogMetadataEvictionAttemptLimit]++
		}
		refs = refs[removeCount:]
		pathBytes = e.cachePathBytesLocked()
	}

	if e.cacheMaxPathBytes <= 0 || pathBytes <= e.cacheMaxPathBytes {
		return evictions
	}
	for _, ref := range refs {
		if pathBytes <= e.cacheMaxPathBytes {
			break
		}
		entry := e.attempts[ref]
		entryBytes := logMetadataPathBytes(entry)
		if entryBytes == 0 {
			continue
		}
		pathBytes -= entryBytes
		entry.parent.paths = nil
		entry.steps.paths = nil
		evictions[LogMetadataEvictionPathBytes]++
	}
	return evictions
}

func (e *logMetadataEnricher) prune(currentTime time.Time) {
	e.mu.Lock()
	evictions := e.enforceLimitsLocked(currentTime)
	observation := e.cacheObservationLocked(currentTime)
	e.mu.Unlock()
	e.observeEvictions(evictions)
	e.observer.ObserveLogMetadataCache(observation)
}

func (e *logMetadataEnricher) sortedAttemptRefsLocked() []cluster.JobAttemptRef {
	refs := make([]cluster.JobAttemptRef, 0, len(e.attempts))
	for ref := range e.attempts {
		refs = append(refs, ref)
	}
	slices.SortFunc(refs, func(left, right cluster.JobAttemptRef) int {
		leftEntry := e.attempts[left]
		rightEntry := e.attempts[right]
		switch {
		case leftEntry.lastObservedAt.Before(rightEntry.lastObservedAt):
			return -1
		case leftEntry.lastObservedAt.After(rightEntry.lastObservedAt):
			return 1
		case left.JobID < right.JobID:
			return -1
		case left.JobID > right.JobID:
			return 1
		case left.AttemptIndex < right.AttemptIndex:
			return -1
		case left.AttemptIndex > right.AttemptIndex:
			return 1
		default:
			return 0
		}
	})
	return refs
}

func (e *logMetadataEnricher) cachePathBytesLocked() int {
	total := 0
	for _, entry := range e.attempts {
		total += logMetadataPathBytes(entry)
	}
	return total
}

func (e *logMetadataEnricher) cacheObservationLocked(
	currentTime time.Time,
) LogMetadataCacheObservation {
	oldestQueuedAt := time.Time{}
	for _, entry := range e.attempts {
		if entry.queued && (oldestQueuedAt.IsZero() || entry.queuedAt.Before(oldestQueuedAt)) {
			oldestQueuedAt = entry.queuedAt
		}
	}
	oldestQueuedAge := time.Duration(0)
	if !oldestQueuedAt.IsZero() && currentTime.After(oldestQueuedAt) {
		oldestQueuedAge = currentTime.Sub(oldestQueuedAt)
	}
	return LogMetadataCacheObservation{
		PathBytes:       e.cachePathBytesLocked(),
		Attempts:        len(e.attempts),
		PathByteLimit:   e.cacheMaxPathBytes,
		AttemptLimit:    e.cacheMaxEntries,
		QueueDepth:      len(e.workQueue),
		QueueCapacity:   cap(e.workQueue),
		WorkersInFlight: e.workersInFlight,
		WorkerLimit:     e.parallelism,
		OldestQueuedAge: oldestQueuedAge,
	}
}

func (e *logMetadataEnricher) observeEvictions(evictions map[LogMetadataEvictionReason]int) {
	for reason, count := range evictions {
		if count > 0 {
			e.observer.ObserveLogMetadataEviction(reason, count)
		}
	}
}

func logMetadataPathBytes(entry *attemptLogMetadata) int {
	total := 0
	for _, paths := range [][]string{entry.parent.paths, entry.steps.paths} {
		for _, path := range paths {
			total += len(path)
		}
	}
	return total
}

func (e *logMetadataEnricher) failCandidate(
	candidate logRefreshCandidate,
	currentTime time.Time,
) {
	e.mu.Lock()
	entry, ok := e.attempts[candidate.ref]
	if !ok || entry.generation != candidate.generation {
		e.mu.Unlock()
		return
	}
	if candidate.parentDue {
		entry.parent.fail(currentTime, e.retryBase, e.refreshInterval)
	}
	if candidate.stepsDue {
		entry.steps.fail(currentTime, e.retryBase, e.refreshInterval)
	}
	evictions := e.enforceLimitsLocked(currentTime)
	observation := e.cacheObservationLocked(currentTime)
	e.mu.Unlock()
	e.observeEvictions(evictions)
	e.observer.ObserveLogMetadataCache(observation)
}

// updateCandidate applies results only to the candidate's live cache generation.
// Incomplete sources retain cached and newly discovered paths while backing off;
// complete sources succeed independently of failures elsewhere in the response.
func (e *logMetadataEnricher) updateCandidate(
	candidate logRefreshCandidate,
	paths sacctAttemptLogPaths,
	currentTime time.Time,
) {
	e.mu.Lock()
	entry, ok := e.attempts[candidate.ref]
	if !ok || entry.generation != candidate.generation {
		e.mu.Unlock()
		return
	}
	if candidate.parentDue {
		if paths.missingParent || (paths.invalidParent > 0 && len(paths.parent) == 0) {
			entry.parent.paths = unionLogPaths(entry.parent.paths, paths.parent)
			entry.parent.fail(currentTime, e.retryBase, e.refreshInterval)
		} else {
			entry.parent.succeed(paths.parent, currentTime)
		}
	}
	if candidate.stepsDue {
		if paths.missingSteps || (paths.invalidSteps > 0 && len(paths.steps) == 0) {
			entry.steps.paths = unionLogPaths(entry.steps.paths, paths.steps)
			entry.steps.fail(currentTime, e.retryBase, e.refreshInterval)
		} else {
			entry.steps.succeed(paths.steps, currentTime)
		}
	}
	evictions := e.enforceLimitsLocked(currentTime)
	observation := e.cacheObservationLocked(currentTime)
	e.mu.Unlock()
	if paths.invalidParent > 0 {
		e.logInvalid(candidate.ref, "sacct_parent", paths.invalidParent)
	}
	if paths.invalidSteps > 0 {
		e.logInvalid(candidate.ref, "sacct_steps", paths.invalidSteps)
	}
	e.observeEvictions(evictions)
	e.observer.ObserveLogMetadataPathsDiscovered(LogMetadataCacheSourceParent, len(paths.parent))
	e.observer.ObserveLogMetadataPathsDiscovered(LogMetadataCacheSourceSteps, len(paths.steps))
	e.observer.ObserveLogMetadataCache(observation)
}

func (e *logMetadataEnricher) publishLocked(job *cluster.JobExecutionRecord) int {
	entry, ok := e.attempts[jobAttemptRef(*job)]
	if !ok {
		return 0
	}
	paths := unionLogPaths(entry.parent.paths, entry.steps.paths)
	if len(paths) == 0 {
		delete(job.SchedulerMetadata, slurmLogFilePathsMetadataKey)
		return 0
	}
	job.SchedulerMetadata[slurmLogFilePathsMetadataKey] = paths
	return len(paths)
}

func (e *logMetadataEnricher) failActiveSource(
	work logMetadataWork,
	source LogMetadataCacheSource,
) bool {
	currentTime := e.clock()
	e.mu.Lock()
	entry, ok := e.liveWorkEntryLocked(work)
	if !ok {
		e.mu.Unlock()
		return false
	}
	if source == LogMetadataCacheSourceParent {
		entry.parent.fail(currentTime, e.retryBase, e.refreshInterval)
	} else {
		entry.steps.fail(currentTime, e.retryBase, e.refreshInterval)
	}
	evictions := e.enforceLimitsLocked(currentTime)
	observation := e.cacheObservationLocked(currentTime)
	e.mu.Unlock()
	e.observeEvictions(evictions)
	e.observer.ObserveLogMetadataCache(observation)
	return true
}

func (e *logMetadataEnricher) applyActiveSacct(
	work logMetadataWork,
	paths sacctAttemptLogPaths,
) bool {
	currentTime := e.clock()
	e.mu.Lock()
	entry, ok := e.liveWorkEntryLocked(work)
	if !ok {
		e.mu.Unlock()
		return false
	}
	if paths.missingSteps || (paths.invalidSteps > 0 && len(paths.steps) == 0) {
		entry.steps.paths = unionLogPaths(entry.steps.paths, paths.steps)
		entry.steps.fail(currentTime, e.retryBase, e.refreshInterval)
	} else {
		entry.steps.succeed(paths.steps, currentTime)
	}
	if work.parentDue && len(paths.parent) > 0 && len(entry.parent.paths) == 0 {
		entry.parent.succeed(paths.parent, currentTime)
	}
	evictions := e.enforceLimitsLocked(currentTime)
	observation := e.cacheObservationLocked(currentTime)
	e.mu.Unlock()
	if paths.invalidParent > 0 {
		e.logInvalid(work.ref, "sacct_parent", paths.invalidParent)
	}
	if paths.invalidSteps > 0 {
		e.logInvalid(work.ref, "sacct_steps", paths.invalidSteps)
	}
	e.observeEvictions(evictions)
	e.observer.ObserveLogMetadataPathsDiscovered(LogMetadataCacheSourceParent, len(paths.parent))
	e.observer.ObserveLogMetadataPathsDiscovered(LogMetadataCacheSourceSteps, len(paths.steps))
	e.observer.ObserveLogMetadataCache(observation)
	return true
}

func (e *logMetadataEnricher) parentFallbackDue(work logMetadataWork) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	entry, ok := e.liveWorkEntryLocked(work)
	return ok && len(entry.parent.paths) == 0
}

func (e *logMetadataEnricher) succeedActiveParent(
	work logMetadataWork,
	paths []string,
) bool {
	currentTime := e.clock()
	e.mu.Lock()
	entry, ok := e.liveWorkEntryLocked(work)
	if !ok {
		e.mu.Unlock()
		return false
	}
	entry.parent.succeed(paths, currentTime)
	evictions := e.enforceLimitsLocked(currentTime)
	observation := e.cacheObservationLocked(currentTime)
	e.mu.Unlock()
	e.observeEvictions(evictions)
	e.observer.ObserveLogMetadataPathsDiscovered(LogMetadataCacheSourceParent, len(paths))
	e.observer.ObserveLogMetadataCache(observation)
	return true
}

func (e *logMetadataEnricher) liveWorkEntryLocked(
	work logMetadataWork,
) (*attemptLogMetadata, bool) {
	entry, ok := e.attempts[work.ref]
	return entry, ok && entry.generation == work.generation && entry.inFlight
}

func (e *logMetadataEnricher) logFailure(
	ref cluster.JobAttemptRef,
	source string,
	err error,
) {
	e.logger.Warn(
		"Slurm log metadata refresh failed",
		zap.String("source", source),
		zap.String("job_id", ref.JobID),
		zap.Int32("attempt_index", ref.AttemptIndex),
		zap.Error(err),
	)
}

func (e *logMetadataEnricher) logInvalid(
	ref cluster.JobAttemptRef,
	source string,
	count int,
) {
	e.logger.Warn(
		"ignored invalid Slurm log metadata values",
		zap.String("source", source),
		zap.String("job_id", ref.JobID),
		zap.Int32("attempt_index", ref.AttemptIndex),
		zap.Int("invalid_value_count", count),
	)
}

func (source *logMetadataSource) due(
	currentTime time.Time,
	stateChanged bool,
	refreshInterval time.Duration,
) bool {
	if stateChanged {
		return true
	}
	if source.failures > 0 {
		return !currentTime.Before(source.nextRetryAt)
	}
	if !source.initialized {
		return true
	}
	return !currentTime.Before(source.refreshedAt.Add(refreshInterval))
}

// dueParent keeps a discovered parent allocation path stable for the lifetime
// of an attempt. Slurm step paths can appear later and are refreshed
// periodically, but the parent StdOut path does not need repeated accounting
// lookups once a non-empty value has been validated.
func (source *logMetadataSource) dueParent(
	currentTime time.Time,
	stateChanged bool,
	refreshInterval time.Duration,
) bool {
	if len(source.paths) > 0 {
		return false
	}
	return source.due(currentTime, stateChanged, refreshInterval)
}

func cacheResult(source logMetadataSource, due bool) LogMetadataCacheResult {
	if !source.initialized {
		return LogMetadataCacheMiss
	}
	if due {
		return LogMetadataCacheStale
	}
	return LogMetadataCacheHit
}

func (source *logMetadataSource) succeed(paths []string, currentTime time.Time) {
	source.paths = append(source.paths[:0], paths...)
	source.initialized = true
	source.refreshedAt = currentTime
	source.lastAttemptAt = currentTime
	source.nextRetryAt = time.Time{}
	source.failures = 0
}

func (source *logMetadataSource) fail(
	currentTime time.Time,
	retryBase time.Duration,
	refreshInterval time.Duration,
) {
	source.failures++
	source.lastAttemptAt = currentTime
	delay := retryBase
	for i := uint(1); i < source.failures && delay < refreshInterval; i++ {
		if delay >= refreshInterval/2 {
			delay = refreshInterval
			break
		}
		delay *= 2
	}
	if delay > refreshInterval {
		delay = refreshInterval
	}
	source.nextRetryAt = currentTime.Add(delay)
}

type logMetadataCommandRunner func(context.Context, string, ...string) (commandOutput, error)

func normalizeDiscoveredPaths(raw []string) ([]string, int) {
	unique := make(map[string]struct{}, len(raw))
	invalid := 0
	for _, path := range raw {
		normalized, err := normalizeSlurmLogFilePaths([]string{path})
		if err != nil {
			invalid++
			continue
		}
		unique[normalized[0]] = struct{}{}
	}

	paths := make([]string, 0, len(unique))
	for path := range unique {
		paths = append(paths, path)
	}
	slices.Sort(paths)
	if len(paths) > maxSlurmLogFilePaths {
		invalid += len(paths) - maxSlurmLogFilePaths
		paths = paths[:maxSlurmLogFilePaths]
	}
	return paths, invalid
}

func unionLogPaths(sources ...[]string) []string {
	unique := make(map[string]struct{})
	for _, paths := range sources {
		for _, path := range paths {
			unique[path] = struct{}{}
		}
	}
	out := make([]string, 0, len(unique))
	for path := range unique {
		out = append(out, path)
	}
	slices.Sort(out)
	if len(out) > maxSlurmLogFilePaths {
		out = out[:maxSlurmLogFilePaths]
	}
	return out
}

func normalizeSlurmLogFilePaths(paths []string) ([]string, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	if len(paths) > maxSlurmLogFilePaths {
		return nil, errors.New("too many Slurm log file paths")
	}

	unique := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		switch {
		case path == "":
			return nil, errors.New("Slurm log file path is empty")
		case !utf8.ValidString(path):
			return nil, errors.New("Slurm log file path is not valid UTF-8")
		case len(path) > maxSlurmLogFilePathBytes:
			return nil, errors.New("Slurm log file path is too long")
		case strings.IndexByte(path, 0) >= 0:
			return nil, errors.New("Slurm log file path contains NUL")
		}
		unique[path] = struct{}{}
	}

	normalized := make([]string, 0, len(unique))
	for path := range unique {
		normalized = append(normalized, path)
	}
	slices.Sort(normalized)
	return normalized, nil
}

// logMetadataFromScontrol returns both the parent path and Slurm's restart
// counter. Workers must validate the counter before writing the exact-attempt
// cache because a requeue can occur while the command is in flight.
func logMetadataFromScontrol(data []byte) (
	stdout string,
	attemptIndex int32,
	stdoutFound bool,
	attemptFound bool,
) {
	for _, field := range strings.Fields(string(data)) {
		if value, ok := strings.CutPrefix(field, "StdOut="); ok {
			stdout = value
			stdoutFound = true
			continue
		}
		if value, ok := strings.CutPrefix(field, "Restarts="); ok {
			parsed, err := strconv.ParseInt(value, 10, 32)
			if err == nil {
				attemptIndex = int32(parsed)
				attemptFound = true
			}
		}
	}
	return stdout, attemptIndex, stdoutFound, attemptFound
}

func classifyLogMetadataCommandOutcome(
	ctx context.Context,
	err error,
) LogMetadataCommandOutcome {
	switch {
	case err == nil:
		return LogMetadataCommandSuccess
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return LogMetadataCommandTimeout
	case errors.Is(ctx.Err(), context.Canceled):
		return LogMetadataCommandCanceled
	default:
		return LogMetadataCommandError
	}
}

func numericStepID(value string) bool {
	if value == "" {
		return false
	}
	_, err := strconv.ParseUint(value, 10, 64)
	return err == nil
}

func jobAttemptRef(job cluster.JobExecutionRecord) cluster.JobAttemptRef {
	return cluster.JobAttemptRef{
		JobID:        job.JobID,
		AttemptIndex: job.AttemptIndex,
	}
}

func oldestAttempt(
	entry *attemptLogMetadata,
	parentDue bool,
	stepsDue bool,
) time.Time {
	var oldest time.Time
	for _, source := range []struct {
		due   bool
		value *logMetadataSource
	}{
		{due: parentDue, value: &entry.parent},
		{due: stepsDue, value: &entry.steps},
	} {
		if !source.due {
			continue
		}
		if source.value.lastAttemptAt.IsZero() {
			return time.Time{}
		}
		if oldest.IsZero() || source.value.lastAttemptAt.Before(oldest) {
			oldest = source.value.lastAttemptAt
		}
	}
	return oldest
}

func earlierRefreshTime(left, right time.Time) time.Time {
	if left.IsZero() || right.IsZero() {
		return time.Time{}
	}
	if left.Before(right) {
		return left
	}
	return right
}

func sortRefreshCandidates(candidates []logRefreshCandidate) {
	slices.SortStableFunc(candidates, func(left, right logRefreshCandidate) int {
		leftKey := left.ref.JobID + "\x00" + strconv.FormatInt(int64(left.ref.AttemptIndex), 10)
		rightKey := right.ref.JobID + "\x00" + strconv.FormatInt(int64(right.ref.AttemptIndex), 10)
		return compareRefreshOrder(left.lastAttempt, leftKey, right.lastAttempt, rightKey)
	})
}

func compareRefreshOrder(
	leftTime time.Time,
	leftKey string,
	rightTime time.Time,
	rightKey string,
) int {
	switch {
	case leftTime.IsZero() && !rightTime.IsZero():
		return -1
	case !leftTime.IsZero() && rightTime.IsZero():
		return 1
	case leftTime.Before(rightTime):
		return -1
	case leftTime.After(rightTime):
		return 1
	case leftKey < rightKey:
		return -1
	case leftKey > rightKey:
		return 1
	default:
		return 0
	}
}
