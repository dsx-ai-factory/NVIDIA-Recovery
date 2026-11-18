// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"slices"
	"time"

	"github.com/cockroachdb/errors"
	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	enrollmentLabel = "recovery.nvidia.com/enabled"
	jobIDLabel      = "recovery.nvidia.com/job-id"
	attemptLabel    = "recovery.nvidia.com/attempt-index"

	// backendCallTimeout bounds each gateway call made by a reconcile.
	backendCallTimeout = 10 * time.Second
	// cacheLagRetry revisits a name whose live object is enrolled with the
	// tracked UID but has not reached the filtered informer cache yet.
	cacheLagRetry = 5 * time.Second
	// unenrolledRecheck revisits a name whose live object is unenrolled but
	// has an open tracked attempt. The filtered cache has no event for that
	// object's deletion or replacement, so the recheck detects it.
	unenrolledRecheck = time.Minute
	// lateCommitGrace delays the read-back of an ambiguous open write until
	// that write is no longer expected to commit.
	lateCommitGrace = 2 * time.Minute
	// lateCommitReads bounds consecutive failed reads of one read-back.
	lateCommitReads = 3
)

var (
	jobSetKind     = schema.GroupVersionKind{Group: "jobset.x-k8s.io", Version: "v1alpha2", Kind: "JobSet"}
	pyTorchJobKind = schema.GroupVersionKind{Group: "kubeflow.org", Version: "v1", Kind: "PyTorchJob"}
	batchJobKind   = schema.GroupVersionKind{Group: "batch", Version: "v1", Kind: "Job"}
)

// jobBackend is the subset of the gateway job API used for publication,
// closure verification, and startup inventory.
type jobBackend interface {
	CreateOrUpdateJob(context.Context, *pb.CreateOrUpdateJobRequest, ...grpc.CallOption) (*pb.CreateOrUpdateJobResponse, error)
	GetJob(context.Context, *pb.GetJobRequest, ...grpc.CallOption) (*pb.GetJobResponse, error)
	GetJobs(context.Context, *pb.GetJobsRequest, ...grpc.CallOption) (*pb.GetJobsResponse, error)
}

// trackedAttempt is the controller's in-memory knowledge of one attempt of
// one workload object, identified by Kubernetes UID. It lives until verified
// loss of that UID is closed with a terminal outcome the backend confirms for
// the exact attempt and UID. Unenrollment does not end tracking, so a recheck
// of the name or a same-name replacement can still close the attempt.
type trackedAttempt struct {
	// uid is the exact workload object whose disappearance closes the row.
	uid types.UID
	// last is the latest observation sent or recovered from the backend.
	last *pb.CreateOrUpdateJobRequest
	// confirmed reports that the backend acknowledged last. For a terminal
	// observation, acknowledgment requires a response showing a terminal row
	// for this attempt and UID.
	confirmed bool
	// closure is the pending terminal upsert: a terminal observation as first
	// sent, or the outcome fixed when loss of uid was verified. It is resent
	// unchanged until acknowledged, so neither Pod cleanup nor a retry changes
	// the outcome or node evidence being recorded.
	closure *pb.CreateOrUpdateJobRequest
	// verify reports that the latest terminal write had an ambiguous outcome.
	// The next attempt reads the exact row before writing again.
	verify bool
	// rejected reports a permanent terminal-write rejection or workload UID
	// conflict. Tracking is retained, but the write is not retried.
	rejected bool
	// lateUntil is set by an ambiguous open write, which can commit after a
	// later acknowledged write and replace its row unseen by deduplication.
	// After lateUntil, a deduplicated observation is read back once. Zero
	// means no read-back is due.
	lateUntil time.Time
	// lateReadFailures counts consecutive failed read-backs.
	lateReadFailures int
}

// clearLate ends the attempt's read-back.
func (t *trackedAttempt) clearLate() {
	t.lateUntil, t.lateReadFailures = time.Time{}, 0
}

// pending reports a terminal write that still needs acknowledgment and may be
// retried.
func (t *trackedAttempt) pending() bool {
	return t.closure != nil && !t.rejected
}

// open reports an attempt whose verified UID loss would still require a
// terminal write: it is neither pending, rejected, nor confirmed terminal.
func (t *trackedAttempt) open() bool {
	if t.closure != nil || t.rejected {
		return false
	}
	return !t.confirmed || !isTerminal(t.last.GetState())
}

// attemptKey distinguishes retries of the same workload and objects that
// have reused a name. The canonical ID includes the job ID and attempt index.
type attemptKey struct {
	uid types.UID
	id  string
}

func (t *trackedAttempt) key() attemptKey {
	return attemptKey{uid: t.uid, id: t.last.GetHeimdallJobId()}
}

// attempts holds observations by workload name and attempt identity until
// their disappearance has been reconciled.
type attempts map[types.NamespacedName]map[attemptKey]*trackedAttempt

func (a attempts) put(name types.NamespacedName, tracked *trackedAttempt) {
	if a[name] == nil {
		a[name] = make(map[attemptKey]*trackedAttempt)
	}
	a[name][tracked.key()] = tracked
}

func (a attempts) forget(name types.NamespacedName, tracked *trackedAttempt) {
	delete(a[name], tracked.key())
	if len(a[name]) == 0 {
		delete(a, name)
	}
}

type workloadReconciler struct {
	// cache is the label-filtered informer cache.
	cache client.Reader
	// live reads the API server directly. A filtered-cache miss is not
	// deletion evidence; only a live read can distinguish deletion, same-name
	// replacement, unenrollment, and an unavailable kind.
	live      client.Reader
	backend   jobBackend
	kind      schema.GroupVersionKind
	kinds     []schema.GroupVersionKind
	logger    *zap.Logger
	placement *placementIndex
	// tracked is owned by the single reconcile worker for this kind.
	tracked attempts
	// now is the clock for late-commit checks; nil uses time.Now.
	now func() time.Time
}

func (r *workloadReconciler) currentTime() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

// Reconcile publishes the current attempt of one workload name and closes
// tracked attempts whose exact workload UID no longer exists. Each kind has one
// worker, which owns the tracking state. The queue retries returned errors
// with backoff, except permanent closure rejections.
func (r *workloadReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	workload := workloadObject(r.kind)
	err := r.cache.Get(ctx, req.NamespacedName, workload)
	switch {
	case err == nil:
		return r.reconcilePresent(ctx, req.NamespacedName, workload)
	case apierrors.IsNotFound(err):
		return r.reconcileMissing(ctx, req.NamespacedName)
	default:
		return reconcile.Result{}, errors.Wrap(err, "read cached workload")
	}
}

// reconcilePresent closes attempts of replaced objects with the same name,
// retries pending terminal writes of the present object, then publishes the
// cached object's current observation.
func (r *workloadReconciler) reconcilePresent(ctx context.Context, name types.NamespacedName, workload *unstructured.Unstructured) (reconcile.Result, error) {
	var errs []error
	for key, tracked := range r.tracked[name] {
		switch {
		case key.uid != workload.GetUID():
			// The name now resolves to another UID, so the tracked object is gone.
			errs = append(errs, r.close(ctx, name, tracked))
		case tracked.pending():
			errs = append(errs, r.confirmTerminal(ctx, name, tracked))
		}
	}
	result, err := r.publish(ctx, name, workload)
	return r.reconcileResult(name, result, append(errs, err))
}

// reconcileMissing resolves a filtered-cache miss with a live read. Only an
// exact UID absence closes a tracked attempt.
func (r *workloadReconciler) reconcileMissing(ctx context.Context, name types.NamespacedName) (reconcile.Result, error) {
	if len(r.tracked[name]) == 0 {
		return reconcile.Result{}, nil
	}
	live, err := r.liveWorkload(ctx, name)
	if err != nil {
		// Outages and uninstalled kinds are not deletion evidence.
		return reconcile.Result{}, err
	}
	return r.reconcileLive(ctx, name, live)
}

// reconcileLive applies a live read of the name, nil when absent, to its
// tracked attempts. It closes attempts whose UID is gone and schedules a
// revisit while the live object keeps a tracked UID outside the cache.
func (r *workloadReconciler) reconcileLive(ctx context.Context, name types.NamespacedName, live *unstructured.Unstructured) (reconcile.Result, error) {
	var result reconcile.Result
	var errs []error
	for key, tracked := range r.tracked[name] {
		switch {
		case live == nil || live.GetUID() != key.uid:
			errs = append(errs, r.close(ctx, name, tracked))
		case tracked.pending():
			// The object still exists, so only its own observed outcome is
			// written; failures return to the queue.
			errs = append(errs, r.confirmTerminal(ctx, name, tracked))
		case live.GetLabels()[enrollmentLabel] == "true":
			result = requeueBy(result, cacheLagRetry)
		case tracked.open():
			// Unenrollment stops publication; the recheck closes the attempt
			// once its UID is gone.
			r.logger.Debug("workload is not enrolled; attempt stays open",
				zap.String("workload", name.String()), zap.String("job_id", tracked.last.GetJobId()))
			result = requeueBy(result, unenrolledRecheck)
		}
	}
	return r.reconcileResult(name, result, errs)
}

// requeueBy returns result scheduled no later than after.
func requeueBy(result reconcile.Result, after time.Duration) reconcile.Result {
	if result.RequeueAfter == 0 || after < result.RequeueAfter {
		result.RequeueAfter = after
	}
	return result
}

// reconcileResult combines independent per-attempt failures with a scheduled
// revisit. Controller-runtime ignores the result when an error is returned and
// does not requeue an error chain that contains a TerminalError. A transient
// failure therefore takes precedence, since its backoff revisits the name.
// Without one, a scheduled revisit takes precedence over permanent rejections
// so another attempt of the same name is still rechecked. Permanent rejections
// left out of the returned chain are logged here; a rejected attempt is not
// resent on a later reconcile.
func (r *workloadReconciler) reconcileResult(name types.NamespacedName, result reconcile.Result, errs []error) (reconcile.Result, error) {
	var transient, permanent []error
	for _, err := range errs {
		switch {
		case err == nil:
		case errors.Is(err, reconcile.TerminalError(nil)):
			permanent = append(permanent, err)
		default:
			transient = append(transient, err)
		}
	}
	if len(transient) == 0 && result.RequeueAfter == 0 {
		return result, errors.Join(permanent...)
	}
	for _, err := range permanent {
		r.logger.Error("workload closure needs operator attention", zap.String("workload", name.String()), zap.Error(err))
	}
	if len(transient) == 0 {
		return result, nil
	}
	return reconcile.Result{}, errors.Join(transient...)
}

// liveWorkload lists by name rather than getting by name: an empty list is
// exact absence, while a 404 means the resource type itself is not served.
func (r *workloadReconciler) liveWorkload(ctx context.Context, name types.NamespacedName) (*unstructured.Unstructured, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(r.kind.GroupVersion().WithKind(r.kind.Kind + "List"))
	err := r.live.List(ctx, list, client.InNamespace(name.Namespace), client.MatchingFields{"metadata.name": name.Name})
	if meta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
		return nil, errors.Wrapf(err, "workload kind %s is not served", r.kind.String())
	}
	if err != nil {
		return nil, errors.Wrap(err, "read live workload")
	}
	for i := range list.Items {
		if list.Items[i].GetName() == name.Name {
			return &list.Items[i], nil
		}
	}
	return nil, nil
}

// publish sends the cached workload's observation when it differs from the
// last confirmed write for that attempt.
//
// Node membership follows live Pods only while the attempt runs and its
// workload is not being deleted. Otherwise the tracked nodes are kept and
// newly observed nodes are added, because Pods are removed during teardown,
// suspension, and after termination. The rule depends only on the current
// observation and the tracked nodes, so it also holds for attempts recovered
// from startup inventory.
//
// Pod events can arrive before the workload event that explains them, so a
// cached running object does not prove that a node removal is a running
// change. Before dropping any tracked node, publish verifies the workload with
// one live read.
//
// A deduplicated observation after an ambiguous open write goes through
// checkLateCommit instead.
func (r *workloadReconciler) publish(ctx context.Context, name types.NamespacedName, workload *unstructured.Unstructured) (reconcile.Result, error) {
	id := workload.GetLabels()[jobIDLabel]
	if id != "" {
		var count int
		for _, kind := range r.kinds {
			matches, err := workloadsWithID(ctx, r.cache, kind, name.Namespace, id)
			if err != nil {
				return reconcile.Result{}, err
			}
			count += len(matches)
		}
		if count > 1 {
			r.logger.Warn("skipping duplicate workload job identity", zap.String("workload", name.String()))
			return reconcile.Result{}, nil
		}
	}

	update, err := observation(ctx, workload, r.placement)
	if err != nil {
		r.logger.Warn("skipping workload observation", zap.String("workload", name.String()), zap.Error(err))
		return reconcile.Result{}, nil
	}
	next := &trackedAttempt{uid: workload.GetUID(), last: update}
	tracked := r.tracked[name][next.key()]
	if tracked == nil {
		return r.sent(next, r.send(ctx, name, next))
	}
	if tracked.closure != nil || (tracked.confirmed && isTerminal(tracked.last.GetState())) {
		// A terminal outcome, pending or confirmed, survives Pod cleanup.
		return reconcile.Result{}, nil
	}
	keep := keepsNodes(workload, update.GetState())
	if !keep && removesNodes(tracked.last.GetAllocatedNodes(), update.GetAllocatedNodes()) {
		live, err := r.liveWorkload(ctx, name)
		switch {
		case err != nil:
			// The published nodes stand until the removal can be verified.
			return reconcile.Result{}, err
		case tracksEnrolled(live, tracked) && live.GetDeletionTimestamp() != nil:
			keep = true
		case !r.runningAttempt(ctx, live, tracked):
			// The cache is stale. The live read already decides closure and
			// revisits, as it would after a cache miss.
			return r.reconcileLive(ctx, name, live)
		}
	}
	if keep {
		update.AllocatedNodes = unionNodes(tracked.last.GetAllocatedNodes(), update.GetAllocatedNodes())
	}
	if tracked.confirmed && proto.Equal(tracked.last, update) {
		return r.checkLateCommit(ctx, name, tracked, update, keep)
	}
	return r.sent(next, r.send(ctx, name, next))
}

// sent schedules the read-back carried by an acknowledged open write.
func (r *workloadReconciler) sent(next *trackedAttempt, err error) (reconcile.Result, error) {
	if err != nil || !next.confirmed || next.lateUntil.IsZero() {
		return reconcile.Result{}, err
	}
	return requeueBy(reconcile.Result{}, max(next.lateUntil.Sub(r.currentTime()), time.Second)), nil
}

// checkLateCommit reads back an acknowledged observation that publish would
// otherwise deduplicate. A read before lateUntil proves nothing, since the
// ambiguous write can still replace the row, so it only reschedules. A row with
// the same state and node set, a terminal row, or a row of another UID or
// attempt ends the read-back without a write; Job Manager would fence or reject
// one. A missing or different open row is repaired from the current
// observation. After lateCommitReads failed reads it settles without reading.
func (r *workloadReconciler) checkLateCommit(ctx context.Context, name types.NamespacedName, tracked *trackedAttempt, update *pb.CreateOrUpdateJobRequest, keep bool) (reconcile.Result, error) {
	if tracked.lateUntil.IsZero() {
		return reconcile.Result{}, nil
	}
	if wait := tracked.lateUntil.Sub(r.currentTime()); wait > 0 {
		return requeueBy(reconcile.Result{}, wait), nil
	}
	if tracked.lateReadFailures >= lateCommitReads {
		return r.repairLateCommit(ctx, name, tracked, update, keep, nil, false)
	}
	row, err := r.storedAttempt(ctx, update.GetHeimdallJobId())
	if err != nil {
		tracked.lateReadFailures++
		return reconcile.Result{}, errors.Wrap(err, "read back ambiguous open write")
	}
	tracked.lateReadFailures = 0
	switch classifyRow(row, tracked.uid, update) {
	case rowConflict:
		r.logger.Warn("stored attempt belongs to another workload or attempt; not repairing it",
			zap.String("workload", name.String()), zap.String("heimdall_job_id", update.GetHeimdallJobId()))
		tracked.clearLate()
		return reconcile.Result{}, nil
	case rowTerminal:
		tracked.clearLate()
		return reconcile.Result{}, nil
	}
	if row != nil && row.GetState() == update.GetState() && sameNodes(row.GetNodes(), update.GetAllocatedNodes()) {
		tracked.clearLate()
		return reconcile.Result{}, nil
	}
	return r.repairLateCommit(ctx, name, tracked, update, keep, row.GetNodes(), true)
}

// repairLateCommit writes the current observation over a row that a late open
// write may have replaced. stored is the row's node set when known.
//
// Dropping a stored node is a node removal, so it needs the same live
// confirmation of a running, enrolled, undeleted attempt as publish. Otherwise
// the stored nodes are kept; when they are unknown, nothing is written.
func (r *workloadReconciler) repairLateCommit(ctx context.Context, name types.NamespacedName, tracked *trackedAttempt, update *pb.CreateOrUpdateJobRequest, keep bool, stored []string, known bool) (reconcile.Result, error) {
	if !keep && (!known || removesNodes(stored, update.GetAllocatedNodes())) {
		live, err := r.liveWorkload(ctx, name)
		switch {
		case err != nil:
			return reconcile.Result{}, err
		case tracksEnrolled(live, tracked) && live.GetDeletionTimestamp() != nil:
			keep = true
		case !r.runningAttempt(ctx, live, tracked):
			return r.reconcileLive(ctx, name, live)
		}
	}
	if keep && !known {
		r.logger.Warn("stored attempt is unreadable and its nodes cannot be dropped; not rewriting it",
			zap.String("workload", name.String()), zap.String("heimdall_job_id", update.GetHeimdallJobId()))
		tracked.clearLate()
		return reconcile.Result{}, nil
	}
	if keep {
		update.AllocatedNodes = unionNodes(stored, update.GetAllocatedNodes())
	}
	// An ambiguous repair starts a new read-back in send.
	tracked.clearLate()
	next := &trackedAttempt{uid: tracked.uid, last: update}
	return r.sent(next, r.send(ctx, name, next))
}

// sameNodes reports whether two node lists hold the same set.
func sameNodes(a, b []string) bool {
	return slices.Equal(unionNodes(a, nil), unionNodes(b, nil))
}

// keepsNodes reports whether an observation keeps the attempt's tracked nodes
// rather than following live Pods.
func keepsNodes(workload *unstructured.Unstructured, state pb.JobStatus) bool {
	return workload.GetDeletionTimestamp() != nil || state != pb.JobStatus_JOB_STATUS_RUNNING
}

// removesNodes reports whether any kept node is missing from observed.
func removesNodes(kept, observed []string) bool {
	present := make(map[string]struct{}, len(observed))
	for _, node := range observed {
		present[node] = struct{}{}
	}
	for _, node := range kept {
		if _, ok := present[node]; !ok {
			return true
		}
	}
	return false
}

// tracksEnrolled reports whether live is the enrolled object of tracked.
func tracksEnrolled(live *unstructured.Unstructured, tracked *trackedAttempt) bool {
	return live != nil && live.GetUID() == tracked.uid && live.GetLabels()[enrollmentLabel] == "true"
}

// runningAttempt reports whether live confirms tracked as an enrolled, running
// attempt that is not being deleted. Only then does a node removal follow
// live Pods.
func (r *workloadReconciler) runningAttempt(ctx context.Context, live *unstructured.Unstructured, tracked *trackedAttempt) bool {
	if !tracksEnrolled(live, tracked) || live.GetDeletionTimestamp() != nil {
		return false
	}
	current, err := observation(ctx, live, r.placement)
	return err == nil && current.GetHeimdallJobId() == tracked.last.GetHeimdallJobId() &&
		current.GetState() == pb.JobStatus_JOB_STATUS_RUNNING
}

// unionNodes returns the sorted union of two node lists.
func unionNodes(kept, observed []string) []string {
	nodes := slices.Concat(kept, observed)
	slices.Sort(nodes)
	return slices.Compact(nodes)
}

// send publishes next.last for a present workload and records the outcome. A
// rejected open write did not commit, so tracking keeps the previous state.
// Any other failure is ambiguous: the server may commit later, so the attempt
// stays tracked as unconfirmed and the error reaches the queue for retry.
//
// A terminal observation is the same outcome closure would record, so it is
// tracked as the pending terminal write before it is sent and goes through the
// same acknowledgment path. It is resent unchanged until the backend shows a
// terminal row for this attempt and UID, because Pod cleanup would otherwise
// change the node evidence of a first write that never committed. A rejected
// terminal write stays tracked as rejected and is not later closed as DELETED.
//
// An ambiguous open write starts or extends the attempt's read-back. An
// acknowledged open write keeps a read-back that is not yet due, because the
// ambiguous write can still replace it, and ends one that is due.
func (r *workloadReconciler) send(ctx context.Context, name types.NamespacedName, next *trackedAttempt) error {
	if isTerminal(next.last.GetState()) {
		next.closure = proto.Clone(next.last).(*pb.CreateOrUpdateJobRequest)
		r.tracked.put(name, next)
		return r.confirmTerminal(ctx, name, next)
	}
	prev := r.tracked[name][next.key()]
	callCtx, cancel := context.WithTimeout(ctx, backendCallTimeout)
	defer cancel()
	_, err := r.backend.CreateOrUpdateJob(callCtx, next.last)
	now := r.currentTime()
	switch {
	case err == nil:
		next.confirmed = true
		if prev != nil && now.Before(prev.lateUntil) {
			next.lateUntil = prev.lateUntil
		}
		r.tracked.put(name, next)
		return nil
	case status.Code(err) == codes.InvalidArgument:
		r.logger.Warn("backend rejected workload observation",
			zap.String("workload", name.String()), zap.String("job_id", next.last.GetJobId()), zap.Error(err))
		return nil
	default:
		next.lateUntil = now.Add(lateCommitGrace)
		r.tracked.put(name, next)
		return errors.Wrap(err, "publish workload observation")
	}
}

// acknowledge records a confirmed terminal outcome for a present workload.
// The attempt stays tracked so later Pod cleanup cannot rewrite it; verified
// UID loss then forgets it without another write.
func (t *trackedAttempt) acknowledge() {
	t.last, t.closure, t.confirmed, t.verify = t.closure, nil, true, false
}

// confirmTerminal retries the pending terminal observation of a workload that
// still exists. It never substitutes DELETED: only verified UID loss does.
func (r *workloadReconciler) confirmTerminal(ctx context.Context, name types.NamespacedName, tracked *trackedAttempt) error {
	if err := r.commitTerminal(ctx, name, tracked); err != nil {
		return err
	}
	tracked.acknowledge()
	return nil
}

// close records the terminal outcome of a tracked attempt whose workload UID
// no longer exists, and stops tracking it only when the backend confirms a
// terminal row for the same attempt and UID.
//
// An observed terminal outcome is replayed as observed; any other attempt
// becomes DELETED with its last known nodes and no invented end time. The
// write is sent whether or not an earlier open write committed: the attempt
// existed, and Job Manager's row lock and terminal-state guard keep the
// outcome if a delayed open write commits afterward.
//
// After an ambiguous closure write, the next call reads the exact canonical
// attempt before writing. A terminal row for the same UID acknowledges the
// closure, an open or missing row needs the write again, and read or write
// failures return to the queue for backoff. A permanent rejection or a row
// owned by another workload UID returns a TerminalError and is not retried.
func (r *workloadReconciler) close(ctx context.Context, name types.NamespacedName, tracked *trackedAttempt) error {
	if tracked.rejected {
		return nil
	}
	if tracked.closure == nil {
		if tracked.confirmed && isTerminal(tracked.last.GetState()) {
			r.tracked.forget(name, tracked)
			return nil
		}
		// The terminal guard fences late open writes.
		tracked.clearLate()
		tracked.closure = proto.Clone(tracked.last).(*pb.CreateOrUpdateJobRequest)
		if !isTerminal(tracked.closure.GetState()) {
			tracked.closure.State = pb.JobStatus_JOB_STATUS_DELETED
			delete(tracked.closure.GetSchedulerMetadata().GetFields(), "termination_reason")
		}
	}
	if err := r.commitTerminal(ctx, name, tracked); err != nil {
		return err
	}
	r.tracked.forget(name, tracked)
	return nil
}

// commitTerminal writes tracked.closure and returns nil only when the backend
// shows a terminal row for the same attempt and UID. After an ambiguous write
// it first reads the exact canonical attempt, so an acknowledged outcome is
// not written again. A permanent rejection or a row owned by another workload
// marks the attempt rejected and returns a TerminalError.
func (r *workloadReconciler) commitTerminal(ctx context.Context, name types.NamespacedName, tracked *trackedAttempt) error {
	if tracked.verify {
		row, err := r.storedAttempt(ctx, tracked.closure.GetHeimdallJobId())
		if err != nil {
			return err
		}
		if done, err := r.settle(name, tracked, classifyRow(row, tracked.uid, tracked.closure), "read"); done {
			return err
		}
	}

	callCtx, cancel := context.WithTimeout(ctx, backendCallTimeout)
	defer cancel()
	response, err := r.backend.CreateOrUpdateJob(callCtx, tracked.closure)
	switch {
	case status.Code(err) == codes.InvalidArgument:
		tracked.rejected = true
		return reconcile.TerminalError(errors.Wrapf(err, "backend rejected terminal outcome of workload %s attempt %s",
			name.String(), tracked.closure.GetHeimdallJobId()))
	case err != nil:
		tracked.verify = true
		return errors.Wrap(err, "publish terminal workload outcome")
	}
	if done, err := r.settle(name, tracked, classifyRow(response.GetJob(), tracked.uid, tracked.closure), "response"); done {
		return err
	}
	// A successful call whose response does not show the terminal row is as
	// ambiguous as a lost reply.
	tracked.verify = true
	return errors.New("terminal write response did not confirm a terminal row")
}

// settle applies a backend row classification to a pending terminal write. It
// reports done when the write is acknowledged or permanently conflicted;
// otherwise the caller writes it again. The caller decides whether an
// acknowledged attempt stays tracked.
func (r *workloadReconciler) settle(name types.NamespacedName, tracked *trackedAttempt, ack rowAcknowledgment, source string) (bool, error) {
	switch ack {
	case rowTerminal:
		return true, nil
	case rowConflict:
		tracked.rejected = true
		return true, reconcile.TerminalError(errors.Newf("backend %s for workload %s attempt %s belongs to another workload or attempt",
			source, name.String(), tracked.closure.GetHeimdallJobId()))
	default:
		return false, nil
	}
}

// storedAttempt returns the backend row for an exact canonical attempt ID, or
// nil when the backend reports it absent.
func (r *workloadReconciler) storedAttempt(ctx context.Context, heimdallJobID string) (*pb.Job, error) {
	callCtx, cancel := context.WithTimeout(ctx, backendCallTimeout)
	defer cancel()
	response, err := r.backend.GetJob(callCtx, &pb.GetJobRequest{HeimdallJobId: heimdallJobID, UseHeimdallId: new(true)})
	if status.Code(err) == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "read backend attempt")
	}
	return response.GetJob(), nil
}

// rowAcknowledgment classifies what a backend row says about a terminal write.
type rowAcknowledgment int

const (
	// rowPending means the row is missing or not positively terminal, so the
	// terminal write is still needed.
	rowPending rowAcknowledgment = iota
	// rowTerminal means the row holds a terminal outcome for the same attempt
	// and workload UID. The outcome may be an earlier one that Job Manager
	// kept in preference to this write.
	rowTerminal
	// rowConflict means the row belongs to another workload UID or attempt.
	rowConflict
)

func (a rowAcknowledgment) String() string {
	switch a {
	case rowTerminal:
		return "terminal"
	case rowConflict:
		return "conflicting"
	default:
		return "not terminal"
	}
}

// classifyRow compares a backend row with the attempt identity of a request
// and the workload UID that owns it. The gateway echoes the requested
// canonical ID in upsert responses, so job ID and attempt index are checked
// as stored values.
func classifyRow(row *pb.Job, uid types.UID, want *pb.CreateOrUpdateJobRequest) rowAcknowledgment {
	if row == nil {
		return rowPending
	}
	if row.GetJobId() != want.GetJobId() || row.GetAttemptIndex() != want.GetAttemptIndex() ||
		(row.GetHeimdallJobId() != "" && row.GetHeimdallJobId() != want.GetHeimdallJobId()) ||
		row.GetSchedulerMetadata().GetFields()["workload_uid"].GetStringValue() != string(uid) {
		return rowConflict
	}
	if isTerminal(row.GetState()) {
		return rowTerminal
	}
	return rowPending
}

// isOpen reports whether a published Kubernetes state can still change.
func isOpen(state pb.JobStatus) bool {
	return state == pb.JobStatus_JOB_STATUS_PENDING || state == pb.JobStatus_JOB_STATUS_RUNNING
}

// isTerminal positively recognizes a final job state. An unspecified or
// unknown state is neither open nor terminal.
func isTerminal(state pb.JobStatus) bool {
	switch state {
	case pb.JobStatus_JOB_STATUS_STOPPED, pb.JobStatus_JOB_STATUS_COMPLETED, pb.JobStatus_JOB_STATUS_FAILED,
		pb.JobStatus_JOB_STATUS_CANCELLED, pb.JobStatus_JOB_STATUS_DELETED:
		return true
	default:
		return false
	}
}

func workloadObject(kind schema.GroupVersionKind) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(kind)
	return obj
}

func workloadsWithID(ctx context.Context, reader client.Reader, kind schema.GroupVersionKind, namespace, id string) ([]unstructured.Unstructured, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(kind.GroupVersion().WithKind(kind.Kind + "List"))
	if err := reader.List(ctx, list, client.InNamespace(namespace), client.MatchingFields{jobIDLabel: id}); err != nil {
		return nil, errors.Wrap(err, "read cached workloads by job ID")
	}
	return list.Items, nil
}

func ownerOfKind(owner *metav1.OwnerReference, kind schema.GroupVersionKind) bool {
	if owner == nil || owner.Kind != kind.Kind {
		return false
	}
	gv, err := schema.ParseGroupVersion(owner.APIVersion)
	return err == nil && gv.Group == kind.Group
}
