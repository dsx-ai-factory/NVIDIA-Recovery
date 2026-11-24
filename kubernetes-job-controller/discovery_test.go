// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const firstJobID = "7e913d4d-98fa-4468-937d-15a7a925155e"
const secondJobID = "4c77dded-38bd-4380-ae79-f739f84bd765"

// gatewayFailure makes one CreateOrUpdateJob call fail. A committed failure
// models a server that persisted the row but whose response was lost. A
// delayed failure models a write that commits only when the test releases it.
type gatewayFailure struct {
	code    codes.Code
	commit  bool
	delayed bool
}

// observationGateway is a behavioral fake of the gateway job API. It keeps
// committed rows so closure verification and startup inventory read the same
// state that publication wrote. Like Job Manager storage, it rejects a write
// that would change an attempt's workload UID and keeps a stored terminal
// outcome against later writes.
type observationGateway struct {
	pb.UnimplementedHeimdallServiceServer
	writes       chan *pb.CreateOrUpdateJobRequest
	mu           sync.Mutex
	rows         map[string]*pb.Job
	failures     []gatewayFailure
	delayed      []*pb.CreateOrUpdateJobRequest
	listFailures int
	getFailures  []codes.Code
	getJobCalls  int
}

func (g *observationGateway) failNext(code codes.Code, commit bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.failures = append(g.failures, gatewayFailure{code: code, commit: commit})
}

// delayNext makes the next write time out at the caller and stay uncommitted
// until commitDelayed.
func (g *observationGateway) delayNext() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.failures = append(g.failures, gatewayFailure{code: codes.DeadlineExceeded, delayed: true})
}

// commitDelayed commits held writes, as a slow server finally would.
func (g *observationGateway) commitDelayed() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, req := range g.delayed {
		_, _ = g.upsert(req)
	}
	g.delayed = nil
}

func (g *observationGateway) CreateOrUpdateJob(_ context.Context, req *pb.CreateOrUpdateJobRequest) (*pb.CreateOrUpdateJobResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	var failure *gatewayFailure
	if len(g.failures) > 0 {
		failure = &g.failures[0]
		g.failures = g.failures[1:]
	}
	switch {
	case failure != nil && failure.delayed:
		g.delayed = append(g.delayed, req)
		return nil, status.Error(failure.code, "injected gateway timeout")
	case failure != nil && !failure.commit:
		return nil, status.Error(failure.code, "injected gateway failure")
	}
	g.writes <- req
	row, err := g.upsert(req)
	if err != nil {
		return nil, err
	}
	if failure != nil {
		return nil, status.Error(failure.code, "injected gateway failure")
	}
	return &pb.CreateOrUpdateJobResponse{Job: row}, nil
}

// upsert applies Job Manager's Kubernetes storage rules and returns the
// stored row.
func (g *observationGateway) upsert(req *pb.CreateOrUpdateJobRequest) (*pb.Job, error) {
	if existing := g.rows[req.GetHeimdallJobId()]; existing != nil {
		if workloadUID(existing.GetSchedulerMetadata()) != workloadUID(req.GetSchedulerMetadata()) {
			return nil, status.Error(codes.InvalidArgument, "conflicting stored Kubernetes workload identity")
		}
		if isTerminal(existing.GetState()) {
			return existing, nil
		}
	}
	g.store(req)
	return g.rows[req.GetHeimdallJobId()], nil
}

func workloadUID(metadata *structpb.Struct) string {
	return metadata.GetFields()["workload_uid"].GetStringValue()
}

func (g *observationGateway) store(req *pb.CreateOrUpdateJobRequest) {
	if g.rows == nil {
		g.rows = make(map[string]*pb.Job)
	}
	g.rows[req.GetHeimdallJobId()] = &pb.Job{
		HeimdallJobId: req.GetHeimdallJobId(), JobId: req.GetJobId(), WorkflowId: req.GetWorkflowId(),
		AttemptIndex: req.GetAttemptIndex(), JobName: req.JobName, State: req.GetState(),
		Nodes: req.GetAllocatedNodes(), SchedulerMetadata: req.GetSchedulerMetadata(),
	}
}

func (g *observationGateway) row(id string) *pb.Job {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.rows[id]
}

func (g *observationGateway) GetJob(_ context.Context, req *pb.GetJobRequest) (*pb.GetJobResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.getJobCalls++
	if len(g.getFailures) > 0 {
		code := g.getFailures[0]
		g.getFailures = g.getFailures[1:]
		return nil, status.Error(code, "injected gateway read failure")
	}
	row, exists := g.rows[req.GetHeimdallJobId()]
	if !req.GetUseHeimdallId() || !exists {
		return nil, status.Error(codes.NotFound, "job not found")
	}
	return &pb.GetJobResponse{Job: row}, nil
}

// GetJobs serves non-final rows in pages of two to exercise continuation.
func (g *observationGateway) GetJobs(_ context.Context, req *pb.GetJobsRequest) (*pb.GetJobsResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.listFailures > 0 {
		g.listFailures--
		return nil, status.Error(codes.Unavailable, "injected gateway outage")
	}
	if req.CompletedJobs == nil || req.GetCompletedJobs() {
		return nil, status.Error(codes.InvalidArgument, "inventory must request open attempts")
	}
	var open []*pb.Job
	for _, id := range slices.Sorted(maps.Keys(g.rows)) {
		if isOpen(g.rows[id].GetState()) {
			open = append(open, g.rows[id])
		}
	}
	offset, _ := strconv.Atoi(req.GetPageToken())
	end := min(offset+2, len(open))
	response := &pb.GetJobsResponse{Jobs: open[offset:end], Count: int32(end - offset)}
	if end < len(open) {
		response.NextPageToken = new(strconv.Itoa(end))
	}
	return response, nil
}

func startGateway(t *testing.T) (*observationGateway, pb.HeimdallServiceClient) {
	t.Helper()
	gateway := &observationGateway{writes: make(chan *pb.CreateOrUpdateJobRequest, 16)}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	pb.RegisterHeimdallServiceServer(server, gateway)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///gateway", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return gateway, pb.NewHeimdallServiceClient(conn)
}

// newReconciler builds a reconciler over fake readers. The live reader
// models the API server, which serves metadata.name field selectors natively;
// the fake client needs an index for them.
func newReconciler(kind schema.GroupVersionKind, kinds []schema.GroupVersionKind, cache, live client.Reader, backend jobBackend) *workloadReconciler {
	return &workloadReconciler{
		cache: cache, live: live, backend: backend, kind: kind, kinds: kinds, logger: zap.NewNop(),
		placement: newPlacementIndex(kind), tracked: make(attempts),
	}
}

func kubeClient(kinds []schema.GroupVersionKind, objects ...client.Object) client.WithWatch {
	builder := fake.NewClientBuilder().WithObjects(objects...)
	for _, kind := range kinds {
		builder = builder.WithIndex(workloadObject(kind), jobIDLabel, jobIDIndex).
			WithIndex(workloadObject(kind), "metadata.name", func(obj client.Object) []string { return []string{obj.GetName()} })
	}
	return builder.Build()
}

// Pod scheduling changes the existing attempt. A copied label cannot add a
// foreign node, and a failed publication must be retried even after deduplication.
func TestReconcilePublishesWorkerPlacementChanges(t *testing.T) {
	jobset := object("jobset.x-k8s.io/v1alpha2", "JobSet", "training", "root-uid", firstJobID)
	jobset.SetLabels(map[string]string{jobIDLabel: firstJobID, enrollmentLabel: "true"})
	// Placement follows live Pods only while the attempt runs.
	jobset.Object["status"] = map[string]any{"replicatedJobsStatus": []any{map[string]any{
		"name": "workers", "active": int64(1), "ready": int64(1), "failed": int64(0),
	}}}
	pytorch := object("kubeflow.org/v1", "PyTorchJob", "pytorch", "pytorch-uid", secondJobID)
	pytorch.SetLabels(map[string]string{jobIDLabel: secondJobID, enrollmentLabel: "true"})
	pytorch.Object["status"] = map[string]any{"conditions": []any{map[string]any{
		"type": "Failed", "status": "True", "reason": "BackoffLimitExceeded",
	}}}
	unstamped := object("jobset.x-k8s.io/v1alpha2", "JobSet", "missing", "missing-uid", "")
	unstamped.SetLabels(map[string]string{enrollmentLabel: "true"})
	job := object("batch/v1", "Job", "training-worker", "owned-job", "")
	addOwner(job, jobset)
	pod := object("v1", "Pod", "worker", "owned-pod", firstJobID)
	addOwner(pod, job)
	foreign := object("v1", "Pod", "foreign", "foreign-pod", firstJobID)
	foreign.Object["spec"] = map[string]any{"nodeName": "foreign-node"}

	podObject := &corev1.Pod{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(pod.Object, podObject); err != nil {
		t.Fatal(err)
	}
	foreignPod := &corev1.Pod{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(foreign.Object, foreignPod); err != nil {
		t.Fatal(err)
	}
	parent := &metav1.PartialObjectMetadata{}
	parent.SetGroupVersionKind(batchJobKind)
	parent.SetName(job.GetName())
	parent.SetNamespace(job.GetNamespace())
	parent.SetUID(job.GetUID())
	parent.SetOwnerReferences(job.GetOwnerReferences())
	kinds := []schema.GroupVersionKind{jobSetKind, pyTorchJobKind}
	kube := kubeClient(kinds, jobset, pytorch, unstamped, parent, podObject, foreignPod)
	gateway, backend := startGateway(t)
	reconciler := func(kind schema.GroupVersionKind) *workloadReconciler {
		return newReconciler(kind, kinds, kube, kube, backend)
	}
	jobSets, torchJobs := reconciler(jobSetKind), reconciler(pyTorchJobKind)
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	t.Cleanup(queue.ShutDown)
	workerEvents := jobSets.placementEvents(jobSets.placement.updatePod)
	parentEvents := jobSets.placementEvents(jobSets.placement.updateJob)
	parentEvents.Create(t.Context(), event.CreateEvent{Object: parent}, queue)
	workerEvents.Create(t.Context(), event.CreateEvent{Object: foreignPod}, queue)
	jobsetRequest := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(jobset)}
	reconcileWorkload(t, jobSets, jobsetRequest)
	first := requireWrite(t, gateway)
	if first.GetJobId() != firstJobID || first.GetWorkflowId() != firstJobID ||
		first.GetHeimdallJobId() != "31e6807a-ac5e-5a26-b4c8-0226d49f8836" || len(first.GetAllocatedNodes()) != 0 {
		t.Fatalf("unexpected unscheduled JobSet observation: %v", first)
	}

	reconcileWorkload(t, torchJobs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(pytorch)})
	second := requireWrite(t, gateway)
	if second.GetJobId() != secondJobID || second.GetState() != pb.JobStatus_JOB_STATUS_FAILED ||
		second.GetSchedulerMetadata().GetFields()["termination_reason"].GetStringValue() != "BackoffLimitExceeded" {
		t.Fatalf("unexpected PyTorchJob terminal observation: %v", second)
	}
	reconcileWorkload(t, jobSets, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(unstamped)})
	reconcileWorkload(t, jobSets, jobsetRequest)
	if len(gateway.writes) != 0 {
		t.Fatal("unstamped or unchanged workload was published")
	}

	unscheduled := podObject.DeepCopy()
	podObject.Spec.NodeName = "owned-node"
	workerEvents.Update(t.Context(), event.UpdateEvent{ObjectOld: unscheduled, ObjectNew: podObject}, queue)
	if queue.Len() != 1 {
		t.Fatalf("node assignment queued %d requests, want one", queue.Len())
	}
	got, _ := queue.Get()
	queue.Done(got)
	if got != jobsetRequest {
		t.Fatalf("Pod update mapped to %v, want %v", got, jobsetRequest)
	}

	gateway.failNext(codes.Unavailable, false)
	if _, err := jobSets.Reconcile(t.Context(), jobsetRequest); status.Code(err) != codes.Unavailable {
		t.Fatalf("failed publication should reach the queue for retry: %v", err)
	}
	reconcileWorkload(t, jobSets, jobsetRequest)
	scheduled := requireWrite(t, gateway)
	if scheduled.GetHeimdallJobId() != first.GetHeimdallJobId() ||
		!slices.Equal(scheduled.GetAllocatedNodes(), []string{"owned-node"}) {
		t.Fatalf("scheduling should update the same attempt with only the verified node: %v", scheduled)
	}
	reconcileWorkload(t, jobSets, jobsetRequest)
	if len(gateway.writes) != 0 {
		t.Fatal("unchanged placement was published again")
	}

	if _, exists := scheduled.SchedulerMetadata.GetFields()["pods"]; exists {
		t.Fatal("the upsert must not carry a per-Pod snapshot")
	}
	sibling := podObject.DeepCopy()
	sibling.Name, sibling.UID = "sibling", "sibling"
	workerEvents.Create(t.Context(), event.CreateEvent{Object: sibling}, queue)
	workerEvents.Delete(t.Context(), event.DeleteEvent{Object: sibling}, queue)
	if queue.Len() != 0 {
		t.Fatal("worker churn on the same node must not queue a job upsert")
	}

	newNode := podObject.DeepCopy()
	newNode.Name, newNode.UID, newNode.Spec.NodeName = "new-worker", "new-worker", "another-node"
	workerEvents.Create(t.Context(), event.CreateEvent{Object: newNode}, queue)
	gateway.failNext(codes.Unavailable, false)
	if _, err := jobSets.Reconcile(t.Context(), jobsetRequest); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected an uncertain publication result: %v", err)
	}
	workerEvents.Delete(t.Context(), event.DeleteEvent{Object: newNode}, queue)
	reconcileWorkload(t, jobSets, jobsetRequest)
	if replay := requireWrite(t, gateway); !proto.Equal(replay, scheduled) {
		t.Fatalf("expected to replay the previous observation after an uncertain write: %v", replay)
	}
	workerEvents.Delete(t.Context(), event.DeleteEvent{Object: podObject}, queue)
	reconcileWorkload(t, jobSets, jobsetRequest)
	removed := requireWrite(t, gateway)
	if removed.GetHeimdallJobId() != first.GetHeimdallJobId() || len(removed.GetAllocatedNodes()) != 0 {
		t.Fatalf("removing the last worker must clear the same attempt's nodes: %v", removed)
	}
	workerEvents.Create(t.Context(), event.CreateEvent{Object: podObject}, queue)

	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(jobset), jobset); err != nil {
		t.Fatal(err)
	}
	labels := jobset.GetLabels()
	labels[attemptLabel] = "1"
	jobset.SetLabels(labels)
	if err := kube.Update(t.Context(), jobset); err != nil {
		t.Fatal(err)
	}
	reconcileWorkload(t, jobSets, jobsetRequest)
	next := requireWrite(t, gateway)
	if next.GetJobId() != firstJobID || next.GetAttemptIndex() != 1 || len(next.GetAllocatedNodes()) != 0 {
		t.Fatalf("new attempt must preserve job identity and exclude the previous worker's node: %v", next)
	}
}

func reconcileWorkload(t *testing.T, r *workloadReconciler, req reconcile.Request) {
	t.Helper()
	if _, err := r.Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
}

func requireWrite(t *testing.T, gateway *observationGateway) *pb.CreateOrUpdateJobRequest {
	t.Helper()
	select {
	case req := <-gateway.writes:
		return req
	default:
		t.Fatal("expected an upsert at the gateway")
		return nil
	}
}

func object(version, kind, name, uid, jobID string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": version, "kind": kind, "metadata": map[string]any{
			"name": name, "namespace": "training", "uid": uid,
		},
	}}
	if jobID != "" {
		obj.SetLabels(map[string]string{jobIDLabel: jobID})
	}
	return obj
}

func addOwner(obj, owner *unstructured.Unstructured) {
	obj.Object["metadata"].(map[string]any)["ownerReferences"] = []any{map[string]any{
		"apiVersion": owner.GetAPIVersion(), "kind": owner.GetKind(),
		"name": owner.GetName(), "uid": string(owner.GetUID()), "controller": true,
	}}
}

// openRow commits an open Kubernetes attempt as an earlier controller run
// would have published it.
func openRow(gateway *observationGateway, namespace, kind, apiVersion, name, uid, jobID string, attempt int32, nodes ...string) *pb.Job {
	metadata, err := structpb.NewStruct(map[string]any{
		"scheduler": "kubernetes", "namespace": namespace, "job_name": name, "job_kind": kind,
		"job_api_version": apiVersion, "workload_uid": uid,
	})
	if err != nil {
		panic(err)
	}
	id := uuid.NewSHA1(uuid.NameSpaceURL, []byte(jobID+"-"+strconv.Itoa(int(attempt)))).String()
	gateway.store(&pb.CreateOrUpdateJobRequest{
		HeimdallJobId: &id, JobId: jobID, WorkflowId: jobID, AttemptIndex: attempt, JobName: &name,
		State: pb.JobStatus_JOB_STATUS_RUNNING, AllocatedNodes: nodes, SchedulerMetadata: metadata,
	})
	return gateway.rows[id]
}

func enrolled(obj *unstructured.Unstructured) *unstructured.Unstructured {
	labels := obj.GetLabels()
	labels[enrollmentLabel] = "true"
	obj.SetLabels(labels)
	return obj
}

func noWrites(t *testing.T, gateway *observationGateway) {
	t.Helper()
	if len(gateway.writes) != 0 {
		t.Fatalf("unexpected upsert: %v", <-gateway.writes)
	}
}

type failingReader struct {
	client.Reader
	err error
}

func (r failingReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return r.err
}

// Restart inventory comes from open backend attempts. Each tracked attempt is
// closed only on exact evidence that its workload UID no longer exists.
func TestStartupInventoryClosesOnlyAbsentWorkloadUIDs(t *testing.T) {
	gateway, backend := startGateway(t)
	jobSetVersion := jobSetKind.GroupVersion().String()
	const (
		deletedID     = "0b9b8c8e-2d0f-4f3e-9d3c-1c1f6f1d2a01"
		replacedID    = "0b9b8c8e-2d0f-4f3e-9d3c-1c1f6f1d2a02"
		replacementID = "0b9b8c8e-2d0f-4f3e-9d3c-1c1f6f1d2a03"
		unenrolledID  = "0b9b8c8e-2d0f-4f3e-9d3c-1c1f6f1d2a04"
		laggingID     = "0b9b8c8e-2d0f-4f3e-9d3c-1c1f6f1d2a05"
	)
	openRow(gateway, "training", "JobSet", jobSetVersion, "deleted", "deleted-uid", deletedID, 0, "node-old")
	openRow(gateway, "training", "JobSet", jobSetVersion, "deleted", "deleted-uid", deletedID, 1, "node-a", "node-b")
	openRow(gateway, "training", "JobSet", jobSetVersion, "replaced", "old-uid", replacedID, 0, "node-c")
	openRow(gateway, "training", "JobSet", jobSetVersion, "unenrolled", "kept-uid", unenrolledID, 0)
	openRow(gateway, "training", "JobSet", jobSetVersion, "lagging", "lagging-uid", laggingID, 0)
	// Rows owned by another namespace, an unserved kind, or a closed attempt
	// are outside this publisher's inventory.
	openRow(gateway, "other", "JobSet", jobSetVersion, "foreign", "foreign-uid", firstJobID, 0)
	openRow(gateway, "training", "PyTorchJob", "kubeflow.org/v1", "unserved", "unserved-uid", secondJobID, 0)
	closed := openRow(gateway, "training", "JobSet", jobSetVersion, "closed", "closed-uid", "closed", 0)
	closed.State = pb.JobStatus_JOB_STATUS_COMPLETED
	gateway.listFailures = 4

	previous := inventoryBackoff
	inventoryBackoff.Duration = time.Millisecond
	inventoryBackoff.Cap = 2 * time.Millisecond
	t.Cleanup(func() { inventoryBackoff = previous })
	kinds := []schema.GroupVersionKind{jobSetKind}
	inventory, err := loadOpenAttempts(t.Context(), backend, "training", kinds, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	names := slices.SortedFunc(maps.Keys(inventory[jobSetKind]), func(a, b types.NamespacedName) int {
		return strings.Compare(a.Name, b.Name)
	})
	if got := fmt.Sprint(names); got != "[training/deleted training/lagging training/replaced training/unenrolled]" || len(inventory) != 1 {
		t.Fatalf("unexpected inventory %v", got)
	}

	replacement := enrolled(object(jobSetVersion, "JobSet", "replaced", "new-uid", replacementID))
	unenrolled := object(jobSetVersion, "JobSet", "unenrolled", "kept-uid", unenrolledID)
	lagging := enrolled(object(jobSetVersion, "JobSet", "lagging", "lagging-uid", laggingID))
	cache := kubeClient(kinds, replacement.DeepCopy())
	live := kubeClient(kinds, replacement, unenrolled, lagging)
	r := newReconciler(jobSetKind, kinds, cache, live, backend)
	r.tracked = inventory[jobSetKind]

	deleted := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "training", Name: "deleted"}}
	for _, failure := range []error{
		&meta.NoKindMatchError{GroupKind: jobSetKind.GroupKind()},
		apierrors.NewServiceUnavailable("API server unavailable"),
	} {
		r.live = failingReader{Reader: live, err: failure}
		if _, err := r.Reconcile(t.Context(), deleted); err == nil {
			t.Fatalf("an unreadable live state must be retried: %v", failure)
		}
		noWrites(t, gateway)
	}
	r.live = live

	reconcileWorkload(t, r, deleted)
	wantNodes := map[int32][]string{0: {"node-old"}, 1: {"node-a", "node-b"}}
	for range 2 {
		closure := requireWrite(t, gateway)
		nodes, expected := wantNodes[closure.GetAttemptIndex()]
		if !expected || closure.GetJobId() != deletedID || closure.GetState() != pb.JobStatus_JOB_STATUS_DELETED ||
			!slices.Equal(closure.GetAllocatedNodes(), nodes) {
			t.Fatalf("deletion while down must close every open attempt with its last nodes: %v", closure)
		}
		delete(wantNodes, closure.GetAttemptIndex())
	}
	noWrites(t, gateway)

	reconcileWorkload(t, r, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(replacement)})
	if old := requireWrite(t, gateway); old.GetJobId() != replacedID || old.GetState() != pb.JobStatus_JOB_STATUS_DELETED {
		t.Fatalf("a same-name replacement must close the old UID's attempt first: %v", old)
	}
	if current := requireWrite(t, gateway); current.GetJobId() != replacementID ||
		current.GetSchedulerMetadata().GetFields()["workload_uid"].GetStringValue() != "new-uid" {
		t.Fatalf("the replacement must publish under its own identity: %v", current)
	}

	for range 2 {
		result, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(unenrolled)})
		if err != nil || result.RequeueAfter == 0 {
			t.Fatalf("an unenrolled workload must stay open and be rechecked: %v %v", result, err)
		}
	}
	noWrites(t, gateway)
	result, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(lagging)})
	if err != nil || result.RequeueAfter == 0 {
		t.Fatalf("an enrolled object missing from the cache must be revisited: %v %v", result, err)
	}
	noWrites(t, gateway)
	if _, tracked := r.tracked[client.ObjectKeyFromObject(unenrolled)]; !tracked {
		t.Fatal("unenrollment must keep the attempt so a same-name replacement can close it")
	}
	if _, tracked := r.tracked[client.ObjectKeyFromObject(lagging)]; !tracked {
		t.Fatal("a cache miss for a live enrolled object must keep its attempt")
	}
}

// Once a terminal outcome is confirmed, Pod cleanup and workload deletion
// must neither rewrite its nodes nor replace the outcome with DELETED.
func TestConfirmedTerminalOutcomeSurvivesCleanup(t *testing.T) {
	gateway, backend := startGateway(t)
	workload := enrolled(object("kubeflow.org/v1", "PyTorchJob", "done", "done-uid", secondJobID))
	workload.Object["status"] = map[string]any{"conditions": []any{map[string]any{
		"type": "Succeeded", "status": "True", "reason": "PyTorchJobSucceeded",
	}}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "worker", Namespace: "training", UID: "worker-uid", Labels: map[string]string{jobIDLabel: secondJobID},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "kubeflow.org/v1", Kind: "PyTorchJob", Name: "done", UID: "done-uid", Controller: new(true),
			}},
		},
		Spec: corev1.PodSpec{NodeName: "node-a"},
	}
	kinds := []schema.GroupVersionKind{pyTorchJobKind}
	kube := kubeClient(kinds, workload)
	r := newReconciler(pyTorchJobKind, kinds, kube, kube, backend)
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	t.Cleanup(queue.ShutDown)
	events := r.placementEvents(r.placement.updatePod)
	request := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(workload)}

	events.Create(t.Context(), event.CreateEvent{Object: pod}, queue)
	reconcileWorkload(t, r, request)
	if done := requireWrite(t, gateway); done.GetState() != pb.JobStatus_JOB_STATUS_COMPLETED ||
		!slices.Equal(done.GetAllocatedNodes(), []string{"node-a"}) {
		t.Fatalf("unexpected terminal observation: %v", done)
	}
	events.Delete(t.Context(), event.DeleteEvent{Object: pod}, queue)
	reconcileWorkload(t, r, request)
	if err := kube.Delete(t.Context(), workload); err != nil {
		t.Fatal(err)
	}
	reconcileWorkload(t, r, request)
	noWrites(t, gateway)
	if len(r.tracked) != 0 {
		t.Fatal("a deleted workload with a confirmed outcome should no longer be tracked")
	}
}

// firstAttemptID is the canonical attempt ID of firstJobID at attempt zero.
const firstAttemptID = "31e6807a-ac5e-5a26-b4c8-0226d49f8836"

// closureFixture is an enrolled JobSet whose deletion the reconciler must
// close.
type closureFixture struct {
	gateway *observationGateway
	r       *workloadReconciler
	kube    client.WithWatch
	request reconcile.Request
}

func newClosureFixture(t *testing.T) *closureFixture {
	t.Helper()
	gateway, backend := startGateway(t)
	workload := enrolled(object(jobSetKind.GroupVersion().String(), "JobSet", "training", "root-uid", firstJobID))
	kinds := []schema.GroupVersionKind{jobSetKind}
	kube := kubeClient(kinds, workload)
	return &closureFixture{
		gateway: gateway, r: newReconciler(jobSetKind, kinds, kube, kube, backend), kube: kube,
		request: reconcile.Request{NamespacedName: client.ObjectKeyFromObject(workload)},
	}
}

func (f *closureFixture) deleteWorkload(t *testing.T) {
	t.Helper()
	workload := workloadObject(jobSetKind)
	if err := f.kube.Get(t.Context(), f.request.NamespacedName, workload); err != nil {
		t.Fatal(err)
	}
	if err := f.kube.Delete(t.Context(), workload); err != nil {
		t.Fatal(err)
	}
}

func drainWrites(gateway *observationGateway) {
	for len(gateway.writes) > 0 {
		<-gateway.writes
	}
}

func isTerminalReconcileError(err error) bool {
	return errors.Is(err, reconcile.TerminalError(nil))
}

// Verified UID loss records DELETED even when the open write's fate is
// unknown. Job Manager keeps the terminal outcome in either commit order, so
// the controller stops tracking once the closure response shows it.
func TestClosureRecordsDeletionInEitherCommitOrder(t *testing.T) {
	for _, tc := range []struct {
		name string
		// fail arranges the outcome of the first open write.
		fail func(*observationGateway)
	}{
		{name: "open write committed before closure", fail: func(g *observationGateway) { g.failNext(codes.DeadlineExceeded, true) }},
		{name: "open write commits after closure", fail: (*observationGateway).delayNext},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newClosureFixture(t)
			tc.fail(f.gateway)
			if _, err := f.r.Reconcile(t.Context(), f.request); err == nil || isTerminalReconcileError(err) {
				t.Fatalf("an ambiguous open write must be retried by the queue: %v", err)
			}
			drainWrites(f.gateway)
			f.deleteWorkload(t)

			reconcileWorkload(t, f.r, f.request)
			if closure := requireWrite(t, f.gateway); closure.GetState() != pb.JobStatus_JOB_STATUS_DELETED ||
				closure.GetHeimdallJobId() != firstAttemptID || closure.GetSchedulerMetadata().GetFields()["workload_uid"].GetStringValue() != "root-uid" {
				t.Fatalf("verified deletion must close the exact attempt and UID: %v", closure)
			}
			if len(f.r.tracked) != 0 || f.gateway.getJobCalls != 0 {
				t.Fatal("a terminal closure response acknowledges the attempt without a read")
			}
			f.gateway.commitDelayed()
			if row := f.gateway.row(firstAttemptID); row.GetState() != pb.JobStatus_JOB_STATUS_DELETED {
				t.Fatalf("a late open write must not reopen the closed attempt: %v", row)
			}
		})
	}
}

// A rejected open write never committed; its workload leaves nothing tracked
// to close.
func TestRejectedOpenWriteLeavesNothingToClose(t *testing.T) {
	f := newClosureFixture(t)
	f.gateway.failNext(codes.InvalidArgument, false)
	reconcileWorkload(t, f.r, f.request)
	f.deleteWorkload(t)
	reconcileWorkload(t, f.r, f.request)
	noWrites(t, f.gateway)
	if len(f.r.tracked) != 0 {
		t.Fatal("a rejected observation must not be tracked")
	}
}

// After an ambiguous closure write, the next reconcile reads the exact
// attempt. Only a terminal row for the same UID acknowledges the closure; an
// open or missing row is written again, read failures back off, and a row
// owned by another workload UID is reported without retry.
func TestAmbiguousClosureIsSettledByExactRead(t *testing.T) {
	for _, tc := range []struct {
		name string
		// openLost loses the first open write, so no row exists.
		openLost bool
		// closureCommits commits the closure whose reply is lost.
		closureCommits bool
		// beforeRead changes backend state before the verifying reconcile.
		beforeRead   func(*observationGateway)
		wantReadErr  bool
		wantConflict bool
		wantRewrite  bool
	}{
		{name: "terminal row acknowledges", closureCommits: true},
		{name: "open row is closed again", wantRewrite: true},
		{name: "missing row is closed", openLost: true, wantRewrite: true},
		{
			// The gateway reports a stored state it does not recognize as
			// unspecified, which is not terminal evidence.
			name: "unrecognized state is closed again", closureCommits: true, wantRewrite: true,
			beforeRead: func(g *observationGateway) { g.rows[firstAttemptID].State = pb.JobStatus_JOB_STATUS_UNSPECIFIED },
		},
		{
			name: "read failure backs off", wantReadErr: true, wantRewrite: true,
			beforeRead: func(g *observationGateway) { g.getFailures = []codes.Code{codes.Unavailable} },
		},
		{
			name: "another workload owns the row", wantConflict: true,
			beforeRead: func(g *observationGateway) {
				g.rows[firstAttemptID].SchedulerMetadata = &structpb.Struct{Fields: map[string]*structpb.Value{
					"scheduler": structpb.NewStringValue("kubernetes"), "workload_uid": structpb.NewStringValue("other-uid"),
				}}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newClosureFixture(t)
			if tc.openLost {
				f.gateway.failNext(codes.Unavailable, false)
				if _, err := f.r.Reconcile(t.Context(), f.request); err == nil {
					t.Fatal("a lost open write must be retried")
				}
			} else {
				reconcileWorkload(t, f.r, f.request)
			}
			drainWrites(f.gateway)
			f.deleteWorkload(t)

			f.gateway.failNext(codes.Unavailable, tc.closureCommits)
			if _, err := f.r.Reconcile(t.Context(), f.request); err == nil || isTerminalReconcileError(err) {
				t.Fatalf("an ambiguous closure must be retried by the queue: %v", err)
			}
			drainWrites(f.gateway)
			if tc.beforeRead != nil {
				tc.beforeRead(f.gateway)
			}

			_, err := f.r.Reconcile(t.Context(), f.request)
			switch {
			case tc.wantConflict:
				if !isTerminalReconcileError(err) {
					t.Fatalf("a UID conflict must be reported without automatic retry: %v", err)
				}
				noWrites(t, f.gateway)
				reads := f.gateway.getJobCalls
				reconcileWorkload(t, f.r, f.request)
				noWrites(t, f.gateway)
				if f.gateway.getJobCalls != reads || len(f.r.tracked) != 1 {
					t.Fatal("a conflicting closure must stay tracked without being retried")
				}
				return
			case tc.wantReadErr:
				if err == nil || isTerminalReconcileError(err) {
					t.Fatalf("a failed read must be retried by the queue: %v", err)
				}
				noWrites(t, f.gateway)
				reconcileWorkload(t, f.r, f.request)
			case err != nil:
				t.Fatal(err)
			}
			if tc.wantRewrite {
				if closure := requireWrite(t, f.gateway); closure.GetState() != pb.JobStatus_JOB_STATUS_DELETED {
					t.Fatalf("an unconfirmed closure must be written again: %v", closure)
				}
			}
			noWrites(t, f.gateway)
			if row := f.gateway.row(firstAttemptID); row.GetState() != pb.JobStatus_JOB_STATUS_DELETED || len(f.r.tracked) != 0 {
				t.Fatalf("closure must end with a DELETED row and no tracking: %v %v", row, f.r.tracked)
			}
		})
	}
}

// A permanently rejected closure stays tracked without being resent, and it
// must not stop the queue from retrying an unrelated transient failure in
// the same reconcile.
func TestRejectedClosureIsNotRetried(t *testing.T) {
	f := newClosureFixture(t)
	reconcileWorkload(t, f.r, f.request)
	drainWrites(f.gateway)
	f.deleteWorkload(t)
	replacement := enrolled(object(jobSetKind.GroupVersion().String(), "JobSet", "training", "new-uid", secondJobID))
	if err := f.kube.Create(t.Context(), replacement); err != nil {
		t.Fatal(err)
	}

	f.gateway.failNext(codes.InvalidArgument, false)
	f.gateway.failNext(codes.Unavailable, false)
	if _, err := f.r.Reconcile(t.Context(), f.request); err == nil || isTerminalReconcileError(err) {
		t.Fatalf("the replacement's failed publication must still be retried: %v", err)
	}
	reconcileWorkload(t, f.r, f.request)
	if current := requireWrite(t, f.gateway); current.GetJobId() != secondJobID {
		t.Fatalf("only the replacement should be published: %v", current)
	}
	noWrites(t, f.gateway)
	if len(f.r.tracked[f.request.NamespacedName]) != 2 {
		t.Fatal("the rejected closure must stay tracked beside the replacement")
	}

	// Without a transient failure, the rejection itself is terminal.
	g := newClosureFixture(t)
	reconcileWorkload(t, g.r, g.request)
	drainWrites(g.gateway)
	g.deleteWorkload(t)
	g.gateway.failNext(codes.InvalidArgument, false)
	if _, err := g.r.Reconcile(t.Context(), g.request); !isTerminalReconcileError(err) {
		t.Fatalf("a rejected closure must not be retried automatically: %v", err)
	}
	reconcileWorkload(t, g.r, g.request)
	noWrites(t, g.gateway)
	if len(g.r.tracked) != 1 {
		t.Fatal("a rejected closure is not an acknowledgment")
	}
}

// pyTorchFixture is an enrolled PyTorchJob with one worker Pod placed on
// node-a. Its native condition decides the published state.
type pyTorchFixture struct {
	gateway *observationGateway
	r       *workloadReconciler
	kube    client.WithWatch
	pods    handler.Funcs
	queue   workqueue.TypedRateLimitingInterface[reconcile.Request]
	pod     *corev1.Pod
	request reconcile.Request
}

func newPyTorchFixture(t *testing.T, condition string, finalizers ...string) *pyTorchFixture {
	t.Helper()
	gateway, backend := startGateway(t)
	workload := enrolled(object("kubeflow.org/v1", "PyTorchJob", "training", "training-uid", secondJobID))
	workload.SetFinalizers(finalizers)
	setCondition(workload, condition)
	kinds := []schema.GroupVersionKind{pyTorchJobKind}
	kube := kubeClient(kinds, workload)
	r := newReconciler(pyTorchJobKind, kinds, kube, kube, backend)
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	t.Cleanup(queue.ShutDown)
	f := &pyTorchFixture{
		gateway: gateway, r: r, kube: kube, pods: r.placementEvents(r.placement.updatePod), queue: queue,
		request: reconcile.Request{NamespacedName: client.ObjectKeyFromObject(workload)},
		pod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "worker", Namespace: "training", UID: "worker-uid", Labels: map[string]string{jobIDLabel: secondJobID},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "kubeflow.org/v1", Kind: "PyTorchJob", Name: "training", UID: "training-uid", Controller: new(true),
				}},
			},
			Spec: corev1.PodSpec{NodeName: "node-a"},
		},
	}
	f.pods.Create(t.Context(), event.CreateEvent{Object: f.pod}, queue)
	return f
}

func setCondition(workload *unstructured.Unstructured, condition string) {
	workload.Object["status"] = map[string]any{"conditions": []any{map[string]any{
		"type": condition, "status": "True", "reason": "Observed" + condition,
	}}}
}

func (f *pyTorchFixture) removePod(t *testing.T) {
	t.Helper()
	f.pods.Delete(t.Context(), event.DeleteEvent{Object: f.pod}, f.queue)
}

// update changes the stored workload, as its operator or a user would.
func (f *pyTorchFixture) update(t *testing.T, change func(*unstructured.Unstructured)) {
	t.Helper()
	workload := workloadObject(pyTorchJobKind)
	if err := f.kube.Get(t.Context(), f.request.NamespacedName, workload); err != nil {
		t.Fatal(err)
	}
	change(workload)
	if err := f.kube.Update(t.Context(), workload); err != nil {
		t.Fatal(err)
	}
}

// markDeleting requests deletion. A finalizer keeps the object, now carrying
// a deletion timestamp, until the test removes it.
func (f *pyTorchFixture) markDeleting(t *testing.T) {
	t.Helper()
	workload := workloadObject(pyTorchJobKind)
	if err := f.kube.Get(t.Context(), f.request.NamespacedName, workload); err != nil {
		t.Fatal(err)
	}
	if err := f.kube.Delete(t.Context(), workload); err != nil {
		t.Fatal(err)
	}
}

func (f *pyTorchFixture) attempt(t *testing.T) *trackedAttempt {
	t.Helper()
	if len(f.r.tracked[f.request.NamespacedName]) != 1 {
		t.Fatalf("expected one tracked attempt: %v", f.r.tracked)
	}
	for _, tracked := range f.r.tracked[f.request.NamespacedName] {
		return tracked
	}
	return nil
}

// A terminal observation whose first write is unacknowledged is resent as
// first observed, even after its workers are cleaned up, until the backend
// shows the terminal row. Loss of the workload replays it rather than DELETED.
func TestPendingTerminalObservationIsResentUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name string
		// fail arranges the outcome of the first terminal write.
		fail func(*observationGateway)
		// deleted removes the workload before the retry.
		deleted bool
		// wantResend expects the retry to write the original payload.
		wantResend bool
	}{
		{name: "write times out before commit", fail: (*observationGateway).delayNext, wantResend: true},
		{name: "write commits but the reply is lost", fail: func(g *observationGateway) { g.failNext(codes.Unavailable, true) }},
		{name: "workload is deleted before acknowledgment", fail: (*observationGateway).delayNext, deleted: true, wantResend: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPyTorchFixture(t, "Failed", "test/hold")
			tc.fail(f.gateway)
			if _, err := f.r.Reconcile(t.Context(), f.request); err == nil || isTerminalReconcileError(err) {
				t.Fatalf("an unacknowledged terminal write must be retried by the queue: %v", err)
			}
			original := proto.Clone(f.attempt(t).last).(*pb.CreateOrUpdateJobRequest)
			if original.GetState() != pb.JobStatus_JOB_STATUS_FAILED || !slices.Equal(original.GetAllocatedNodes(), []string{"node-a"}) {
				t.Fatalf("unexpected terminal observation: %v", original)
			}
			drainWrites(f.gateway)
			f.removePod(t)
			if tc.deleted {
				f.markDeleting(t)
				f.update(t, func(w *unstructured.Unstructured) { w.SetFinalizers(nil) })
			}

			reconcileWorkload(t, f.r, f.request)
			if tc.wantResend {
				if resent := requireWrite(t, f.gateway); !proto.Equal(resent, original) {
					t.Fatalf("the pending terminal write must be resent unchanged: %v", resent)
				}
			} else if f.gateway.getJobCalls != 1 {
				t.Fatalf("a committed write must be acknowledged by one exact read, got %d", f.gateway.getJobCalls)
			}
			noWrites(t, f.gateway)
			row := f.gateway.row(original.GetHeimdallJobId())
			if row.GetState() != pb.JobStatus_JOB_STATUS_FAILED || !slices.Equal(row.GetNodes(), []string{"node-a"}) ||
				row.GetSchedulerMetadata().GetFields()["termination_reason"].GetStringValue() != "ObservedFailed" {
				t.Fatalf("the backend must hold the observed outcome and nodes: %v", row)
			}
			if tc.deleted {
				if len(f.r.tracked) != 0 {
					t.Fatal("an acknowledged closure ends tracking")
				}
				return
			}
			if tracked := f.attempt(t); !tracked.confirmed || tracked.pending() {
				t.Fatalf("the terminal outcome must be confirmed and stay tracked: %+v", tracked)
			}
			reconcileWorkload(t, f.r, f.request)
			noWrites(t, f.gateway)
			f.gateway.commitDelayed()
			if late := f.gateway.row(original.GetHeimdallJobId()); !proto.Equal(late, row) {
				t.Fatalf("a late first write must not change the stored outcome: %v", late)
			}
		})
	}
}

// Foreground deletion removes workers while the workload can still report
// running. Those removals must not shrink the attempt's nodes, and only the
// verified loss of the workload UID records DELETED.
func TestTeardownKeepsLastKnownNodes(t *testing.T) {
	f := newPyTorchFixture(t, "Running", "test/hold")
	reconcileWorkload(t, f.r, f.request)
	running := requireWrite(t, f.gateway)
	if running.GetState() != pb.JobStatus_JOB_STATUS_RUNNING || !slices.Equal(running.GetAllocatedNodes(), []string{"node-a"}) {
		t.Fatalf("unexpected running observation: %v", running)
	}
	f.markDeleting(t)
	f.removePod(t)
	reconcileWorkload(t, f.r, f.request)
	noWrites(t, f.gateway)
	if row := f.gateway.row(running.GetHeimdallJobId()); row.GetState() != pb.JobStatus_JOB_STATUS_RUNNING ||
		!slices.Equal(row.GetNodes(), []string{"node-a"}) {
		t.Fatalf("a deletion timestamp alone must neither close the attempt nor drop its nodes: %v", row)
	}

	f.update(t, func(w *unstructured.Unstructured) { w.SetFinalizers(nil) })
	reconcileWorkload(t, f.r, f.request)
	if closure := requireWrite(t, f.gateway); closure.GetState() != pb.JobStatus_JOB_STATUS_DELETED ||
		!slices.Equal(closure.GetAllocatedNodes(), []string{"node-a"}) {
		t.Fatalf("verified UID loss must close the attempt with its last running nodes: %v", closure)
	}
}

// While running, Pod removals update the attempt. Once it stops running, its
// last known nodes are kept and nodes seen at termination are added.
func TestStoppedAttemptKeepsLastKnownNodes(t *testing.T) {
	f := newPyTorchFixture(t, "Running")
	reconcileWorkload(t, f.r, f.request)
	drainWrites(f.gateway)
	moved := f.pod.DeepCopy()
	moved.Name, moved.UID, moved.Spec.NodeName = "replacement", "replacement-uid", "node-b"
	f.pods.Create(t.Context(), event.CreateEvent{Object: moved}, f.queue)
	f.removePod(t)
	reconcileWorkload(t, f.r, f.request)
	if live := requireWrite(t, f.gateway); !slices.Equal(live.GetAllocatedNodes(), []string{"node-b"}) {
		t.Fatalf("a running attempt must follow live placement: %v", live)
	}

	f.update(t, func(w *unstructured.Unstructured) { setCondition(w, "Suspended") })
	f.pods.Delete(t.Context(), event.DeleteEvent{Object: moved}, f.queue)
	reconcileWorkload(t, f.r, f.request)
	if suspended := requireWrite(t, f.gateway); suspended.GetState() != pb.JobStatus_JOB_STATUS_PENDING ||
		!slices.Equal(suspended.GetAllocatedNodes(), []string{"node-b"}) {
		t.Fatalf("an attempt that stops running keeps its last nodes: %v", suspended)
	}

	f.update(t, func(w *unstructured.Unstructured) { setCondition(w, "Failed") })
	late := f.pod.DeepCopy()
	late.Name, late.UID, late.Spec.NodeName = "late", "late-uid", "node-c"
	f.pods.Create(t.Context(), event.CreateEvent{Object: late}, f.queue)
	reconcileWorkload(t, f.r, f.request)
	if failed := requireWrite(t, f.gateway); failed.GetState() != pb.JobStatus_JOB_STATUS_FAILED ||
		!slices.Equal(failed.GetAllocatedNodes(), []string{"node-b", "node-c"}) {
		t.Fatalf("a terminal observation keeps earlier nodes and adds newly seen ones: %v", failed)
	}
}

// An unenrolled workload keeps its attempt tracked and its name rechecked.
// Re-enrolling the same object resumes publication without a closure, and an
// enrolled same-name replacement closes the old attempt.
func TestUnenrolledAttemptIsClosedByEnrolledReplacement(t *testing.T) {
	gateway, backend := startGateway(t)
	version := jobSetKind.GroupVersion().String()
	kinds := []schema.GroupVersionKind{jobSetKind}
	original := func() *unstructured.Unstructured {
		return enrolled(object(version, "JobSet", "training", "root-uid", firstJobID))
	}
	cache := kubeClient(kinds, original())
	live := kubeClient(kinds, original())
	r := newReconciler(jobSetKind, kinds, cache, live, backend)
	request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "training", Name: "training"}}
	reconcileWorkload(t, r, request)
	drainWrites(gateway)

	// The filtered cache drops an object whose enrollment label is removed.
	setEnrollment := func(enrolled bool) {
		t.Helper()
		workload := workloadObject(jobSetKind)
		if err := live.Get(t.Context(), request.NamespacedName, workload); err != nil {
			t.Fatal(err)
		}
		labels := workload.GetLabels()
		if enrolled {
			labels[enrollmentLabel] = "true"
			if err := cache.Create(t.Context(), original()); err != nil {
				t.Fatal(err)
			}
		} else {
			delete(labels, enrollmentLabel)
			if err := cache.Delete(t.Context(), original()); err != nil {
				t.Fatal(err)
			}
		}
		workload.SetLabels(labels)
		if err := live.Update(t.Context(), workload); err != nil {
			t.Fatal(err)
		}
	}
	unenrolledReconciles := func() {
		t.Helper()
		for range 2 {
			if result, err := r.Reconcile(t.Context(), request); err != nil || result.RequeueAfter == 0 {
				t.Fatalf("an unenrolled workload must stay open and be rechecked: %v %v", result, err)
			}
		}
		noWrites(t, gateway)
	}

	setEnrollment(false)
	unenrolledReconciles()
	setEnrollment(true)
	reconcileWorkload(t, r, request)
	noWrites(t, gateway)
	if row := gateway.row(firstAttemptID); row.GetState() != pb.JobStatus_JOB_STATUS_PENDING || len(r.tracked[request.NamespacedName]) != 1 {
		t.Fatalf("re-enrollment of the same object must not close its attempt: %v", row)
	}

	setEnrollment(false)
	unenrolledReconciles()
	if err := live.Delete(t.Context(), original()); err != nil {
		t.Fatal(err)
	}
	replacement := func() *unstructured.Unstructured {
		return enrolled(object(version, "JobSet", "training", "new-uid", secondJobID))
	}
	if err := live.Create(t.Context(), replacement()); err != nil {
		t.Fatal(err)
	}
	if err := cache.Create(t.Context(), replacement()); err != nil {
		t.Fatal(err)
	}
	reconcileWorkload(t, r, request)
	if closure := requireWrite(t, gateway); closure.GetHeimdallJobId() != firstAttemptID ||
		closure.GetState() != pb.JobStatus_JOB_STATUS_DELETED {
		t.Fatalf("an enrolled replacement must close the unenrolled object's attempt: %v", closure)
	}
	if current := requireWrite(t, gateway); current.GetJobId() != secondJobID {
		t.Fatalf("the replacement must publish under its own identity: %v", current)
	}
	if len(r.tracked[request.NamespacedName]) != 1 {
		t.Fatalf("only the replacement should remain tracked: %v", r.tracked)
	}
}

// A suspended attempt recovered from startup inventory keeps its stored nodes
// although its workers are gone.
func TestRecoveredSuspendedAttemptKeepsStoredNodes(t *testing.T) {
	f := newPyTorchFixture(t, "Suspended")
	f.removePod(t)
	row := openRow(f.gateway, "training", "PyTorchJob", "kubeflow.org/v1", "training", "training-uid", secondJobID, 0, "node-a")
	row.State = pb.JobStatus_JOB_STATUS_PENDING
	inventory, err := loadOpenAttempts(t.Context(), f.r.backend, "training", f.r.kinds, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	f.r.tracked = inventory[pyTorchJobKind]
	reconcileWorkload(t, f.r, f.request)
	drainWrites(f.gateway)
	if stored := f.gateway.row(row.GetHeimdallJobId()); stored.GetState() != pb.JobStatus_JOB_STATUS_PENDING ||
		!slices.Equal(stored.GetNodes(), []string{"node-a"}) {
		t.Fatalf("a recovered attempt that is not running must keep its nodes: %v", stored)
	}
}

// A rejected terminal observation is reported without retry. It replaces any
// earlier open observation of the attempt, so loss of the workload does not
// later record DELETED in its place.
func TestRejectedTerminalObservationIsNotRetriedOrClosed(t *testing.T) {
	for _, tc := range []struct {
		name      string
		priorOpen bool
	}{
		{name: "after an open observation", priorOpen: true},
		{name: "as the first observation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			condition := "Failed"
			if tc.priorOpen {
				condition = "Running"
			}
			f := newPyTorchFixture(t, condition, "test/hold")
			if tc.priorOpen {
				reconcileWorkload(t, f.r, f.request)
				drainWrites(f.gateway)
				f.update(t, func(w *unstructured.Unstructured) { setCondition(w, "Failed") })
			}
			f.gateway.failNext(codes.InvalidArgument, false)
			if _, err := f.r.Reconcile(t.Context(), f.request); !isTerminalReconcileError(err) {
				t.Fatalf("a rejected terminal observation must not be retried automatically: %v", err)
			}
			reconcileWorkload(t, f.r, f.request)
			noWrites(t, f.gateway)

			f.markDeleting(t)
			f.update(t, func(w *unstructured.Unstructured) { w.SetFinalizers(nil) })
			reconcileWorkload(t, f.r, f.request)
			noWrites(t, f.gateway)
			if tracked := f.attempt(t); !tracked.rejected || tracked.closure.GetState() != pb.JobStatus_JOB_STATUS_FAILED {
				t.Fatalf("the rejected terminal outcome must stay tracked: %+v", tracked)
			}
			for _, row := range f.gateway.rows {
				if row.GetState() != pb.JobStatus_JOB_STATUS_RUNNING {
					t.Fatalf("only the earlier open observation may be stored: %v", row)
				}
			}
		})
	}
}

// Deleting an unenrolled workload produces no filtered-cache event, so the
// scheduled recheck of its name is what records the deletion. Once nothing
// open is tracked, the name is no longer rechecked.
func TestUnenrolledWorkloadDeletionIsClosedByRecheck(t *testing.T) {
	gateway, backend := startGateway(t)
	kinds := []schema.GroupVersionKind{jobSetKind}
	workload := enrolled(object(jobSetKind.GroupVersion().String(), "JobSet", "training", "root-uid", firstJobID))
	cache := kubeClient(kinds, workload.DeepCopy())
	live := kubeClient(kinds, workload.DeepCopy())
	r := newReconciler(jobSetKind, kinds, cache, live, backend)
	request := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(workload)}
	reconcileWorkload(t, r, request)
	drainWrites(gateway)

	if err := cache.Delete(t.Context(), workload.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	unenrolled := workload.DeepCopy()
	unenrolled.SetLabels(map[string]string{jobIDLabel: firstJobID})
	unenrolled.SetResourceVersion("")
	if err := live.Delete(t.Context(), workload.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	if err := live.Create(t.Context(), unenrolled); err != nil {
		t.Fatal(err)
	}
	if result, err := r.Reconcile(t.Context(), request); err != nil || result.RequeueAfter == 0 {
		t.Fatalf("an unenrolled open attempt must be rechecked: %v %v", result, err)
	}
	noWrites(t, gateway)

	if err := live.Delete(t.Context(), unenrolled); err != nil {
		t.Fatal(err)
	}
	if result, err := r.Reconcile(t.Context(), request); err != nil || result.RequeueAfter != 0 {
		t.Fatalf("closing the last open attempt must end rechecks: %v %v", result, err)
	}
	if closure := requireWrite(t, gateway); closure.GetHeimdallJobId() != firstAttemptID ||
		closure.GetState() != pb.JobStatus_JOB_STATUS_DELETED {
		t.Fatalf("silent deletion of an unenrolled workload must close its attempt: %v", closure)
	}
	if len(r.tracked) != 0 {
		t.Fatalf("an acknowledged closure ends tracking: %v", r.tracked)
	}
}

// An unenrolled workload whose outcome is already confirmed has nothing left
// to close, so its name is not rechecked.
func TestUnenrolledConfirmedOutcomeIsNotRechecked(t *testing.T) {
	f := newPyTorchFixture(t, "Succeeded")
	reconcileWorkload(t, f.r, f.request)
	drainWrites(f.gateway)
	f.update(t, func(w *unstructured.Unstructured) { w.SetLabels(map[string]string{jobIDLabel: secondJobID}) })
	f.r.cache = kubeClient(f.r.kinds)
	if result, err := f.r.Reconcile(t.Context(), f.request); err != nil || result.RequeueAfter != 0 {
		t.Fatalf("a confirmed outcome must not be rechecked: %v %v", result, err)
	}
	noWrites(t, f.gateway)
}

// A permanent closure rejection for one UID must not cancel the recheck of
// another tracked attempt under the same name.
func TestRejectedClosureKeepsUnenrolledRecheck(t *testing.T) {
	gateway, backend := startGateway(t)
	version := jobSetKind.GroupVersion().String()
	kinds := []schema.GroupVersionKind{jobSetKind}
	openRow(gateway, "training", "JobSet", version, "training", "old-uid", firstJobID, 0)
	openRow(gateway, "training", "JobSet", version, "training", "kept-uid", secondJobID, 0)
	inventory, err := loadOpenAttempts(t.Context(), backend, "training", kinds, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	live := kubeClient(kinds, object(version, "JobSet", "training", "kept-uid", secondJobID))
	r := newReconciler(jobSetKind, kinds, kubeClient(kinds), live, backend)
	r.tracked = inventory[jobSetKind]
	request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "training", Name: "training"}}

	gateway.failNext(codes.InvalidArgument, false)
	for range 2 {
		if result, err := r.Reconcile(t.Context(), request); err != nil || result.RequeueAfter == 0 {
			t.Fatalf("the unenrolled attempt must still be rechecked: %v %v", result, err)
		}
		noWrites(t, gateway)
	}
	if len(r.tracked[request.NamespacedName]) != 2 {
		t.Fatalf("the rejected and unenrolled attempts must both stay tracked: %v", r.tracked)
	}
}

// Pod removals can reach the controller before the workload event that
// explains them. Dropping tracked nodes from a cached running observation
// requires the live workload to confirm a running attempt. Otherwise the last
// node evidence is kept, every attempt of a lost UID is closed with it, or the
// name is revisited once the cache can catch up.
func TestNodeRemovalIsVerifiedAgainstLiveWorkload(t *testing.T) {
	for _, tc := range []struct {
		name string
		// live builds the API server state the reconciler reads directly.
		live func(t *testing.T, cached *unstructured.Unstructured) client.Reader
		// addNode places a new worker on node-b after node-a's worker leaves.
		addNode     bool
		wantClosure bool
		wantRequeue bool
		wantNodes   []string
	}{
		{
			name: "workload is being deleted", addNode: true, wantNodes: []string{"node-a", "node-b"},
			live: func(t *testing.T, cached *unstructured.Unstructured) client.Reader {
				deleting := cached.DeepCopy()
				deleting.SetFinalizers([]string{"test/hold"})
				live := kubeClient([]schema.GroupVersionKind{pyTorchJobKind}, deleting)
				if err := live.Delete(t.Context(), deleting); err != nil {
					t.Fatal(err)
				}
				return live
			},
		},
		{
			name: "workload is deleted", wantClosure: true,
			live: func(*testing.T, *unstructured.Unstructured) client.Reader {
				return kubeClient([]schema.GroupVersionKind{pyTorchJobKind})
			},
		},
		{
			name: "workload is replaced", wantClosure: true,
			live: func(_ *testing.T, cached *unstructured.Unstructured) client.Reader {
				replacement := cached.DeepCopy()
				replacement.SetUID("replacement-uid")
				return kubeClient([]schema.GroupVersionKind{pyTorchJobKind}, replacement)
			},
		},
		{
			name: "workload is unenrolled", wantRequeue: true,
			live: func(_ *testing.T, cached *unstructured.Unstructured) client.Reader {
				unenrolled := cached.DeepCopy()
				unenrolled.SetLabels(map[string]string{jobIDLabel: secondJobID})
				return kubeClient([]schema.GroupVersionKind{pyTorchJobKind}, unenrolled)
			},
		},
		{
			name: "workload stopped running", wantRequeue: true,
			live: func(_ *testing.T, cached *unstructured.Unstructured) client.Reader {
				failed := cached.DeepCopy()
				setCondition(failed, "Failed")
				return kubeClient([]schema.GroupVersionKind{pyTorchJobKind}, failed)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPyTorchFixture(t, "Running")
			reconcileWorkload(t, f.r, f.request)
			running := requireWrite(t, f.gateway)
			cached := workloadObject(pyTorchJobKind)
			if err := f.kube.Get(t.Context(), f.request.NamespacedName, cached); err != nil {
				t.Fatal(err)
			}
			cached.SetResourceVersion("")
			f.r.live = tc.live(t, cached)
			// Another open attempt of the same workload UID.
			other := openRow(f.gateway, "training", "PyTorchJob", "kubeflow.org/v1", "training", "training-uid", secondJobID, 1, "node-old")
			_, _, otherAttempt, _ := inventoryAttempt(other, "training", f.r.kinds)
			f.r.tracked.put(f.request.NamespacedName, otherAttempt)
			f.removePod(t)
			if tc.addNode {
				moved := f.pod.DeepCopy()
				moved.Name, moved.UID, moved.Spec.NodeName = "moved", "moved-uid", "node-b"
				f.pods.Create(t.Context(), event.CreateEvent{Object: moved}, f.queue)
			}

			result, err := f.r.Reconcile(t.Context(), f.request)
			if err != nil || (result.RequeueAfter != 0) != tc.wantRequeue {
				t.Fatalf("unexpected reconcile outcome: %v %v", result, err)
			}
			switch {
			case tc.wantClosure:
				wantNodes := map[int32][]string{0: {"node-a"}, 1: {"node-old"}}
				for range 2 {
					closure := requireWrite(t, f.gateway)
					nodes, expected := wantNodes[closure.GetAttemptIndex()]
					if !expected || closure.GetState() != pb.JobStatus_JOB_STATUS_DELETED || !slices.Equal(closure.GetAllocatedNodes(), nodes) {
						t.Fatalf("UID loss must close every attempt of the UID with its last nodes: %v", closure)
					}
					delete(wantNodes, closure.GetAttemptIndex())
				}
				if len(f.r.tracked) != 0 {
					t.Fatalf("an acknowledged closure ends tracking: %v", f.r.tracked)
				}
			case tc.wantNodes != nil:
				if kept := requireWrite(t, f.gateway); kept.GetState() != pb.JobStatus_JOB_STATUS_RUNNING ||
					!slices.Equal(kept.GetAllocatedNodes(), tc.wantNodes) {
					t.Fatalf("teardown must keep earlier nodes and add new ones: %v", kept)
				}
			default:
				if row := f.gateway.row(running.GetHeimdallJobId()); !slices.Equal(row.GetNodes(), []string{"node-a"}) {
					t.Fatalf("a stale running observation must not drop nodes: %v", row)
				}
			}
			if !tc.wantClosure && len(f.r.tracked[f.request.NamespacedName]) != 2 {
				t.Fatalf("attempts of a present UID must stay tracked: %v", f.r.tracked)
			}
			noWrites(t, f.gateway)
		})
	}
}

// An unreadable live workload is not evidence for a node removal. The prior
// nodes stay published and the queue retries until the removal is verified.
func TestUnverifiedNodeRemovalIsRetried(t *testing.T) {
	f := newPyTorchFixture(t, "Running")
	reconcileWorkload(t, f.r, f.request)
	running := requireWrite(t, f.gateway)
	f.removePod(t)
	for _, failure := range []error{
		&meta.NoKindMatchError{GroupKind: pyTorchJobKind.GroupKind()},
		apierrors.NewServiceUnavailable("API server unavailable"),
	} {
		f.r.live = failingReader{Reader: f.kube, err: failure}
		if _, err := f.r.Reconcile(t.Context(), f.request); err == nil || isTerminalReconcileError(err) {
			t.Fatalf("an unverified node removal must be retried: %v", err)
		}
		noWrites(t, f.gateway)
	}
	if row := f.gateway.row(running.GetHeimdallJobId()); !slices.Equal(row.GetNodes(), []string{"node-a"}) {
		t.Fatalf("an unverified removal must keep the published nodes: %v", row)
	}

	f.r.live = f.kube
	reconcileWorkload(t, f.r, f.request)
	if removed := requireWrite(t, f.gateway); removed.GetState() != pb.JobStatus_JOB_STATUS_RUNNING ||
		len(removed.GetAllocatedNodes()) != 0 {
		t.Fatalf("a verified running removal must be published: %v", removed)
	}
}

// manualClock is a reconciler clock that tests advance explicitly.
type manualClock struct{ now time.Time }

func (c *manualClock) Now() time.Time          { return c.now }
func (c *manualClock) advance(d time.Duration) { c.now = c.now.Add(d) }

func (f *pyTorchFixture) useClock() *manualClock {
	clock := &manualClock{now: time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)}
	f.r.now = clock.Now
	return clock
}

// locked changes fake backend state between reconciles.
func (g *observationGateway) locked(change func()) {
	g.mu.Lock()
	defer g.mu.Unlock()
	change()
}

func (g *observationGateway) reads() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.getJobCalls
}

// placeWorker adds a worker of the fixture's workload on node.
func (f *pyTorchFixture) placeWorker(t *testing.T, node string) *corev1.Pod {
	t.Helper()
	pod := f.pod.DeepCopy()
	pod.Name, pod.UID, pod.Spec.NodeName = "worker-"+node, types.UID("uid-"+node), node
	f.pods.Create(t.Context(), event.CreateEvent{Object: pod}, f.queue)
	return pod
}

// An open write that times out can commit after a later acknowledged write
// and restore nodes the attempt no longer uses. Once the grace period ends,
// the controller reads the row and repairs it from the current observation.
// A node held only by the stored row is dropped only when the live workload is
// a running attempt; during teardown the stored nodes are kept.
func TestLateOpenWriteIsRepairedAfterGrace(t *testing.T) {
	for _, tc := range []struct {
		name string
		// teardown makes deletion visible before the check.
		teardown  func(*testing.T, *pyTorchFixture)
		wantNodes []string
	}{
		{name: "running workload drops the stale node", wantNodes: []string{"node-a"}},
		{
			name: "deleting workload keeps the stale node", wantNodes: []string{"node-a", "node-b"},
			teardown: func(t *testing.T, f *pyTorchFixture) { f.markDeleting(t) },
		},
		{
			name: "deletion seen only by the live read keeps the stale node", wantNodes: []string{"node-a", "node-b"},
			teardown: func(t *testing.T, f *pyTorchFixture) {
				deleting := workloadObject(pyTorchJobKind)
				if err := f.kube.Get(t.Context(), f.request.NamespacedName, deleting); err != nil {
					t.Fatal(err)
				}
				deleting.SetResourceVersion("")
				live := kubeClient(f.r.kinds, deleting)
				if err := live.Delete(t.Context(), deleting); err != nil {
					t.Fatal(err)
				}
				f.r.live = live
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPyTorchFixture(t, "Running", "test/hold")
			clock := f.useClock()
			second := f.placeWorker(t, "node-b")
			f.gateway.delayNext()
			if _, err := f.r.Reconcile(t.Context(), f.request); err == nil {
				t.Fatal("a timed-out open write must be retried")
			}
			clock.advance(10 * time.Second)
			f.pods.Delete(t.Context(), event.DeleteEvent{Object: second}, f.queue)
			result, err := f.r.Reconcile(t.Context(), f.request)
			if err != nil || result.RequeueAfter != lateCommitGrace-10*time.Second {
				t.Fatalf("an acknowledged write after an ambiguous one must schedule the check: %v %v", result, err)
			}
			acked := requireWrite(t, f.gateway)
			if !slices.Equal(acked.GetAllocatedNodes(), []string{"node-a"}) {
				t.Fatalf("unexpected acknowledged observation: %v", acked)
			}
			f.gateway.commitDelayed()
			id := acked.GetHeimdallJobId()
			if row := f.gateway.row(id); !slices.Equal(row.GetNodes(), []string{"node-a", "node-b"}) {
				t.Fatalf("the late write must have replaced the acknowledged row: %v", row)
			}
			if tc.teardown != nil {
				tc.teardown(t, f)
			}

			clock.advance(lateCommitGrace - 11*time.Second)
			result, err = f.r.Reconcile(t.Context(), f.request)
			if err != nil || result.RequeueAfter != time.Second || f.gateway.reads() != 0 {
				t.Fatalf("a check must not read before the grace period ends: %v %v", result, err)
			}
			noWrites(t, f.gateway)

			clock.advance(time.Second)
			reconcileWorkload(t, f.r, f.request)
			repaired := requireWrite(t, f.gateway)
			if f.gateway.reads() != 1 || repaired.GetState() != pb.JobStatus_JOB_STATUS_RUNNING ||
				!slices.Equal(repaired.GetAllocatedNodes(), tc.wantNodes) {
				t.Fatalf("one read must lead to one repair of the current observation: %v", repaired)
			}
			if row := f.gateway.row(id); !slices.Equal(row.GetNodes(), tc.wantNodes) {
				t.Fatalf("the repair must replace the late row: %v", row)
			}

			clock.advance(time.Hour)
			reconcileWorkload(t, f.r, f.request)
			noWrites(t, f.gateway)
			if f.gateway.reads() != 1 {
				t.Fatal("a repair ends the check")
			}
		})
	}
}

// Writes that are acknowledged leave nothing that could commit later, so
// they are never read back.
func TestAcknowledgedWritesAreNotReadBack(t *testing.T) {
	f := newPyTorchFixture(t, "Running")
	clock := f.useClock()
	reconcileWorkload(t, f.r, f.request)
	f.placeWorker(t, "node-b")
	reconcileWorkload(t, f.r, f.request)
	drainWrites(f.gateway)
	clock.advance(time.Hour)
	result, err := f.r.Reconcile(t.Context(), f.request)
	if err != nil || result.RequeueAfter != 0 || f.gateway.reads() != 0 {
		t.Fatalf("an unchanged acknowledged observation needs no I/O: %v %v reads=%d", result, err, f.gateway.reads())
	}
	noWrites(t, f.gateway)
}

// While writes keep failing, each retry is a full write and no read is made.
// Every ambiguous write restarts the grace period, and a matching row ends the
// check after one read without another write.
func TestLateCommitCheckWaitsForTheLastAmbiguousWrite(t *testing.T) {
	f := newPyTorchFixture(t, "Running")
	clock := f.useClock()
	f.gateway.failNext(codes.Unavailable, false)
	if _, err := f.r.Reconcile(t.Context(), f.request); err == nil {
		t.Fatal("a failed write must be retried")
	}
	clock.advance(3 * time.Minute)
	f.gateway.failNext(codes.DeadlineExceeded, true)
	if _, err := f.r.Reconcile(t.Context(), f.request); err == nil {
		t.Fatal("a write without a reply must be retried")
	}
	clock.advance(30 * time.Second)
	result, err := f.r.Reconcile(t.Context(), f.request)
	if err != nil || result.RequeueAfter != lateCommitGrace-30*time.Second {
		t.Fatalf("the check must be due one grace period after the last ambiguous write: %v %v", result, err)
	}
	if f.gateway.reads() != 0 {
		t.Fatal("a write outage must not cause reads")
	}
	drainWrites(f.gateway)

	clock.advance(lateCommitGrace - 31*time.Second)
	if result, err := f.r.Reconcile(t.Context(), f.request); err != nil || result.RequeueAfter != time.Second {
		t.Fatalf("an early revisit must only reschedule the check: %v %v", result, err)
	}
	clock.advance(time.Second)
	if result, err := f.r.Reconcile(t.Context(), f.request); err != nil || result.RequeueAfter != 0 {
		t.Fatalf("a matching row must end the check: %v %v", result, err)
	}
	clock.advance(time.Hour)
	reconcileWorkload(t, f.r, f.request)
	noWrites(t, f.gateway)
	if f.gateway.reads() != 1 {
		t.Fatalf("a matching row must be read exactly once, got %d", f.gateway.reads())
	}
}

// The check never overrides a terminal row or a row owned by another
// workload, recreates a missing row, and after repeated read failures writes
// only an observation the live workload confirms as running.
func TestLateCommitCheckSettlesStoredRow(t *testing.T) {
	for _, tc := range []struct {
		name string
		// before changes the backend or workload before the check is due.
		before func(t *testing.T, f *pyTorchFixture, id string)
		// failedReads is the number of reconciles whose read fails.
		failedReads int
		wantWrite   bool
	}{
		{
			name: "terminal row is kept",
			before: func(_ *testing.T, f *pyTorchFixture, id string) {
				f.gateway.locked(func() { f.gateway.rows[id].State = pb.JobStatus_JOB_STATUS_COMPLETED })
			},
		},
		{
			name: "row of another workload is kept",
			before: func(_ *testing.T, f *pyTorchFixture, id string) {
				f.gateway.locked(func() {
					f.gateway.rows[id].SchedulerMetadata = &structpb.Struct{Fields: map[string]*structpb.Value{
						"scheduler": structpb.NewStringValue("kubernetes"), "workload_uid": structpb.NewStringValue("other-uid"),
					}}
				})
			},
		},
		{
			name: "missing row is recreated", wantWrite: true,
			before: func(_ *testing.T, f *pyTorchFixture, id string) {
				f.gateway.locked(func() { delete(f.gateway.rows, id) })
			},
		},
		{name: "unreadable row of a running workload is rewritten", failedReads: lateCommitReads, wantWrite: true},
		{
			name: "unreadable row of a deleting workload is kept", failedReads: lateCommitReads,
			before: func(t *testing.T, f *pyTorchFixture, _ string) { f.markDeleting(t) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPyTorchFixture(t, "Running", "test/hold")
			clock := f.useClock()
			f.gateway.failNext(codes.Unavailable, true)
			if _, err := f.r.Reconcile(t.Context(), f.request); err == nil {
				t.Fatal("a write without a reply must be retried")
			}
			reconcileWorkload(t, f.r, f.request)
			drainWrites(f.gateway)
			id := f.attempt(t).last.GetHeimdallJobId()
			if tc.before != nil {
				tc.before(t, f, id)
			}
			clock.advance(lateCommitGrace)
			for range tc.failedReads {
				f.gateway.locked(func() { f.gateway.getFailures = []codes.Code{codes.Unavailable} })
				if _, err := f.r.Reconcile(t.Context(), f.request); err == nil || isTerminalReconcileError(err) {
					t.Fatalf("a failed read must be retried by the queue: %v", err)
				}
				noWrites(t, f.gateway)
			}

			reconcileWorkload(t, f.r, f.request)
			if tc.wantWrite {
				if rewritten := requireWrite(t, f.gateway); rewritten.GetState() != pb.JobStatus_JOB_STATUS_RUNNING ||
					!slices.Equal(rewritten.GetAllocatedNodes(), []string{"node-a"}) {
					t.Fatalf("the current observation must be written: %v", rewritten)
				}
			}
			reads := max(tc.failedReads, 1)
			clock.advance(time.Hour)
			reconcileWorkload(t, f.r, f.request)
			noWrites(t, f.gateway)
			if f.gateway.reads() != reads {
				t.Fatalf("the check must end after %d reads, got %d", reads, f.gateway.reads())
			}
		})
	}
}
