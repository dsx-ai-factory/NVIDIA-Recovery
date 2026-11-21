// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/google/uuid"
	"go.uber.org/zap/zaptest"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

const (
	envtestNamespace = "training"
	otherNamespace   = "elsewhere"
	// awaitTimeout bounds each wait for an expected gateway write.
	awaitTimeout = 20 * time.Second
)

// TestManagerAgainstAPIServer runs the production manager from newManager
// against a real API server and etcd started by envtest. It changes the
// cluster only through API writes and asserts only what reaches the gateway.
//
// envtest runs no controllers, scheduler, kubelet, or garbage collector, so
// the test creates every Pod and child Job itself and assigns spec.nodeName.
//
// Subtests share one manager and run in order, each with its own workload
// names and job IDs. Controller-runtime rejects a second controller with the
// same name in one process, so this test runs once per process.
//
// A case that must not publish is checked against an enrolled PyTorchJob
// sentinel created after its action. One informer delivers PyTorchJob events
// in resource-version order and one worker reconciles them in queue order, so
// a write caused by the earlier event would precede the sentinel's first write.
func TestManagerAgainstAPIServer(t *testing.T) {
	c := startCluster(t)

	// State left by an earlier controller run. Each open row carries the UID
	// the API server assigned to its workload.
	deleted := create(t, c.kube, newWorkload(pyTorchJobKind, envtestNamespace, "deleted-while-stopped", uuid.NewString(), true))
	c.delete(t, deleted)
	kept := create(t, c.kube, newWorkload(pyTorchJobKind, envtestNamespace, "unenrolled-while-stopped", uuid.NewString(), false))
	foreign := create(t, c.kube, newWorkload(pyTorchJobKind, otherNamespace, "other-namespace-row", uuid.NewString(), false))
	c.gateway.locked(func() {
		for _, w := range []*unstructured.Unstructured{deleted, kept, foreign} {
			openRow(c.gateway, w.GetNamespace(), w.GetKind(), w.GetAPIVersion(), w.GetName(), string(w.GetUID()), jobID(w), 0, "node-a")
		}
	})

	c.startManager(t)

	t.Run("startup closes only workload UIDs absent from the API server", func(t *testing.T) {
		_, closure := c.writes.await(t, "closure of the workload deleted while stopped",
			attemptIn(deleted, 0, pb.JobStatus_JOB_STATUS_DELETED))
		if !slices.Equal(closure.GetAllocatedNodes(), []string{"node-a"}) {
			t.Errorf("closure nodes = %v, want the stored nodes [node-a]", closure.GetAllocatedNodes())
		}

		// The closure shows the controller has started, so its startup queue
		// already holds the other inventory names ahead of the sentinel.
		_, sentinel := c.publish(t, "startup-sentinel")
		for _, w := range c.writes.before(sentinel) {
			switch w.GetJobId() {
			case jobID(kept):
				t.Errorf("wrote attempt of a live unenrolled workload outside the cache: %v", w)
			case jobID(foreign):
				t.Errorf("wrote attempt recorded in another namespace: %v", w)
			}
		}
	})

	t.Run("publishes an enrolled workload with its API server identity", func(t *testing.T) {
		w := newWorkload(pyTorchJobKind, envtestNamespace, "published", uuid.NewString(), true)
		w.SetLabels(map[string]string{jobIDLabel: jobID(w), enrollmentLabel: "true", attemptLabel: "1"})
		create(t, c.kube, w)
		_, got := c.writes.await(t, "publication", attemptIn(w, 1, pb.JobStatus_JOB_STATUS_PENDING))

		if got.GetJobId() != jobID(w) || got.GetWorkflowId() != jobID(w) || got.GetAttemptIndex() != 1 || got.GetJobName() != w.GetName() {
			t.Errorf("identity = job %q workflow %q attempt %d name %q", got.GetJobId(), got.GetWorkflowId(), got.GetAttemptIndex(), got.GetJobName())
		}
		want := map[string]string{
			"scheduler": "kubernetes", "namespace": envtestNamespace, "job_name": w.GetName(),
			"job_api_version": "kubeflow.org/v1", "job_kind": "PyTorchJob", "workload_uid": string(w.GetUID()),
		}
		fields := got.GetSchedulerMetadata().GetFields()
		for key, value := range want {
			if fields[key].GetStringValue() != value {
				t.Errorf("scheduler metadata %s = %q, want %q", key, fields[key].GetStringValue(), value)
			}
		}
		submitted, err := time.Parse(time.RFC3339Nano, fields["kubernetes_submit_time"].GetStringValue())
		if err != nil || !submitted.Equal(w.GetCreationTimestamp().Time) {
			t.Errorf("kubernetes_submit_time = %v (%v), want creation time %v", submitted, err, w.GetCreationTimestamp().Time)
		}
	})

	t.Run("publishes only enrolled workloads in its namespace", func(t *testing.T) {
		unenrolled := create(t, c.kube, newWorkload(pyTorchJobKind, envtestNamespace, "not-enrolled", uuid.NewString(), false))
		elsewhere := create(t, c.kube, newWorkload(pyTorchJobKind, otherNamespace, "enrolled-elsewhere", uuid.NewString(), true))
		_, sentinel := c.publish(t, "filter-sentinel")
		for _, w := range c.writes.before(sentinel) {
			if w.GetJobId() == jobID(unenrolled) || w.GetJobId() == jobID(elsewhere) {
				t.Errorf("published a workload outside the enrollment or namespace filter: %v", w)
			}
		}

		// Enrollment moves the object into the label-filtered informer.
		c.setEnrollment(t, unenrolled, true)
		c.writes.await(t, "publication after enrollment", attemptIn(unenrolled, 0, pb.JobStatus_JOB_STATUS_PENDING))
	})

	t.Run("publishes the terminal outcome from a status update", func(t *testing.T) {
		w, _ := c.publish(t, "succeeds")
		if err := unstructured.SetNestedSlice(w.Object, []any{map[string]any{
			"type": "Succeeded", "status": "True", "reason": "JobSucceeded",
		}}, "status", "conditions"); err != nil {
			t.Fatal(err)
		}
		if err := c.kube.Status().Update(t.Context(), w); err != nil {
			t.Fatal(err)
		}
		_, got := c.writes.await(t, "completion", attemptIn(w, 0, pb.JobStatus_JOB_STATUS_COMPLETED))
		if reason := got.GetSchedulerMetadata().GetFields()["termination_reason"].GetStringValue(); reason != "JobSucceeded" {
			t.Errorf("termination_reason = %q, want JobSucceeded", reason)
		}
	})

	t.Run("publishes nodes of worker Pods", func(t *testing.T) {
		w, _ := c.publish(t, "placed")
		create(t, c.kube, workerPod(w, pyTorchJobKind, "placed-worker-0", "node-a", jobID(w)))
		c.writes.await(t, "first worker node", withNodes(attemptIn(w, 0, pb.JobStatus_JOB_STATUS_PENDING), "node-a"))
		create(t, c.kube, workerPod(w, pyTorchJobKind, "placed-worker-1", "node-b", jobID(w)))
		c.writes.await(t, "second worker node", withNodes(attemptIn(w, 0, pb.JobStatus_JOB_STATUS_PENDING), "node-a", "node-b"))
	})

	t.Run("publishes nodes of JobSet Pods through their child Job", func(t *testing.T) {
		w := create(t, c.kube, newWorkload(jobSetKind, envtestNamespace, "jobset", uuid.NewString(), true))
		c.writes.await(t, "JobSet publication", attemptIn(w, 0, pb.JobStatus_JOB_STATUS_PENDING))
		job := create(t, c.kube, childJob(w, "jobset-workers"))
		create(t, c.kube, workerPod(job, batchJobKind, "jobset-workers-0", "node-j", jobID(w)))
		c.writes.await(t, "JobSet worker node", withNodes(attemptIn(w, 0, pb.JobStatus_JOB_STATUS_PENDING), "node-j"))
	})

	t.Run("deletion closes the attempt with its last known nodes", func(t *testing.T) {
		w, _ := c.publish(t, "deleted")
		create(t, c.kube, workerPod(w, pyTorchJobKind, "deleted-worker-0", "node-c", jobID(w)))
		c.writes.await(t, "worker node", withNodes(attemptIn(w, 0, pb.JobStatus_JOB_STATUS_PENDING), "node-c"))
		c.delete(t, w)
		c.writes.await(t, "closure", withNodes(attemptIn(w, 0, pb.JobStatus_JOB_STATUS_DELETED), "node-c"))
	})

	t.Run("same-name replacement closes the replaced UID", func(t *testing.T) {
		old, _ := c.publish(t, "replaced")
		c.delete(t, old)
		replacement := create(t, c.kube, newWorkload(pyTorchJobKind, envtestNamespace, old.GetName(), uuid.NewString(), true))
		if replacement.GetUID() == old.GetUID() {
			t.Fatal("replacement reused the deleted UID")
		}
		c.writes.await(t, "closure of the replaced UID", attemptIn(old, 0, pb.JobStatus_JOB_STATUS_DELETED))
		c.writes.await(t, "publication of the replacement", attemptIn(replacement, 0, pb.JobStatus_JOB_STATUS_PENDING))
	})

	t.Run("unenrollment keeps the attempt open", func(t *testing.T) {
		w, _ := c.publish(t, "unenrolled-later")
		// The object leaves the label-filtered informer as a delete event,
		// but a live read still finds the same UID.
		c.setEnrollment(t, w, false)
		_, sentinel := c.publish(t, "unenrollment-sentinel")
		for _, got := range c.writes.before(sentinel) {
			if got.GetHeimdallJobId() == attemptID(jobID(w), 0) && isTerminal(got.GetState()) {
				t.Errorf("closed the attempt of a live unenrolled workload: %v", got)
			}
		}
	})
}

// envtestCluster is one envtest API server with the controller's gateway.
type envtestCluster struct {
	env     *envtest.Environment
	kube    client.Client
	gateway *observationGateway
	backend pb.HeimdallServiceClient
	writes  *writeLog
}

// startCluster starts envtest with the workload CRDs, the namespaces, and a
// fake gateway that records writes. KUBEBUILDER_ASSETS names the API server
// and etcd binaries; without it the test is skipped, so `make test-go`
// still compiles and vets this file. `make test-go-envtest` always sets it.
func startCluster(t *testing.T) *envtestCluster {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset; run make test-go-envtest")
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("testdata", "crds")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Errorf("stop envtest: %v", err)
		}
	})
	version, err := discovery.NewDiscoveryClientForConfigOrDie(cfg).ServerVersion()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("envtest API server %s", version.GitVersion)

	kube, err := client.New(cfg, client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	c := &envtestCluster{env: env, kube: kube}
	for _, name := range []string{envtestNamespace, otherNamespace} {
		create(t, c.kube, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}})
	}
	c.gateway, c.backend = startGateway(t)
	c.writes = recordWrites(t, c.gateway)
	return c
}

// startManager builds the manager exactly as run does after configuration
// and starts it. Cleanup stops the manager before the gateway and envtest.
func (c *envtestCluster) startManager(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	mgr, err := newManager(ctx, c.env.Config, envtestNamespace, c.backend, zaptest.NewLogger(t))
	if err != nil {
		cancel()
		t.Fatalf("build manager: %v", err)
	}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		if err := mgr.Start(ctx); err != nil {
			t.Errorf("manager stopped: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-stopped
	})
}

// create writes obj and returns it with its server-assigned fields.
func create[T client.Object](t *testing.T, kube client.Client, obj T) T {
	t.Helper()
	if err := kube.Create(t.Context(), obj); err != nil {
		t.Fatalf("create %s %s: %v", obj.GetObjectKind().GroupVersionKind().Kind, obj.GetName(), err)
	}
	return obj
}

func (c *envtestCluster) delete(t *testing.T, obj client.Object) {
	t.Helper()
	if err := c.kube.Delete(t.Context(), obj); err != nil {
		t.Fatalf("delete %s: %v", obj.GetName(), err)
	}
}

// publish creates an enrolled PyTorchJob and waits for its first write. It
// returns the workload and the write's position in the log.
func (c *envtestCluster) publish(t *testing.T, name string) (*unstructured.Unstructured, int) {
	t.Helper()
	w := newWorkload(pyTorchJobKind, envtestNamespace, name, uuid.NewString(), true)
	create(t, c.kube, w)
	at, _ := c.writes.await(t, name+" publication", attemptIn(w, 0, pb.JobStatus_JOB_STATUS_PENDING))
	return w, at
}

func (c *envtestCluster) setEnrollment(t *testing.T, w *unstructured.Unstructured, enroll bool) {
	t.Helper()
	patch := client.MergeFrom(w.DeepCopy())
	labels := w.GetLabels()
	if enroll {
		labels[enrollmentLabel] = "true"
	} else {
		delete(labels, enrollmentLabel)
	}
	w.SetLabels(labels)
	if err := c.kube.Patch(t.Context(), w, patch); err != nil {
		t.Fatalf("set enrollment of %s: %v", w.GetName(), err)
	}
}

func newWorkload(kind schema.GroupVersionKind, namespace, name, id string, enroll bool) *unstructured.Unstructured {
	w := workloadObject(kind)
	w.SetNamespace(namespace)
	w.SetName(name)
	labels := map[string]string{jobIDLabel: id}
	if enroll {
		labels[enrollmentLabel] = "true"
	}
	w.SetLabels(labels)
	return w
}

func jobID(w client.Object) string {
	return w.GetLabels()[jobIDLabel]
}

func controllerRef(owner client.Object, kind schema.GroupVersionKind) []metav1.OwnerReference {
	return []metav1.OwnerReference{{
		APIVersion: kind.GroupVersion().String(), Kind: kind.Kind, Name: owner.GetName(), UID: owner.GetUID(), Controller: new(true),
	}}
}

// workerPod is a Pod already bound to node, as the scheduler would leave it.
func workerPod(owner client.Object, ownerKind schema.GroupVersionKind, name, node, id string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: owner.GetNamespace(), Labels: map[string]string{jobIDLabel: id},
			OwnerReferences: controllerRef(owner, ownerKind),
		},
		Spec: corev1.PodSpec{NodeName: node, Containers: []corev1.Container{{Name: "worker", Image: "worker"}}},
	}
}

// childJob is a batch Job controlled by a JobSet, as the JobSet controller
// would create it.
func childJob(owner client.Object, name string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: owner.GetNamespace(), OwnerReferences: controllerRef(owner, jobSetKind)},
		Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: "worker", Image: "worker"}},
		}}},
	}
}

// attemptID is the canonical heimdall_job_id: UUIDv5 in the URL namespace of
// "<job ID>-<attempt index>".
func attemptID(id string, attempt int) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(id+"-"+strconv.Itoa(attempt))).String()
}

type writeMatch func(*pb.CreateOrUpdateJobRequest) bool

// attemptIn matches a write of one attempt of the exact workload object in
// state.
func attemptIn(w client.Object, attempt int, state pb.JobStatus) writeMatch {
	id := attemptID(jobID(w), attempt)
	return func(got *pb.CreateOrUpdateJobRequest) bool {
		return got.GetHeimdallJobId() == id && got.GetState() == state &&
			workloadUID(got.GetSchedulerMetadata()) == string(w.GetUID())
	}
}

func withNodes(match writeMatch, nodes ...string) writeMatch {
	return func(got *pb.CreateOrUpdateJobRequest) bool {
		return match(got) && slices.Equal(got.GetAllocatedNodes(), nodes)
	}
}

// writeLog records gateway upserts in commit order. The fake gateway sends
// each write on a bounded channel while holding its lock, so the log drains
// the channel for the whole test.
type writeLog struct {
	mu      sync.Mutex
	writes  []*pb.CreateOrUpdateJobRequest
	changed chan struct{}
}

func recordWrites(t *testing.T, gateway *observationGateway) *writeLog {
	l := &writeLog{changed: make(chan struct{})}
	done, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case w := <-gateway.writes:
				l.mu.Lock()
				l.writes = append(l.writes, w)
				close(l.changed)
				l.changed = make(chan struct{})
				l.mu.Unlock()
			case <-done:
				return
			}
		}
	}()
	t.Cleanup(func() {
		close(done)
		<-stopped
	})
	return l
}

// await returns the position and value of the first write that matches,
// failing the test if none arrives within awaitTimeout.
func (l *writeLog) await(t *testing.T, what string, match writeMatch) (int, *pb.CreateOrUpdateJobRequest) {
	t.Helper()
	deadline := time.After(awaitTimeout)
	for {
		l.mu.Lock()
		for i, w := range l.writes {
			if match(w) {
				l.mu.Unlock()
				return i, w
			}
		}
		changed := l.changed
		l.mu.Unlock()
		select {
		case <-changed:
		case <-deadline:
			t.Fatalf("no gateway write for %s within %s; writes: %v", what, awaitTimeout, l.before(-1))
		}
	}
}

// before returns the writes ahead of position end, or all writes when end is
// negative.
func (l *writeLog) before(end int) []*pb.CreateOrUpdateJobRequest {
	l.mu.Lock()
	defer l.mu.Unlock()
	if end < 0 {
		end = len(l.writes)
	}
	return slices.Clone(l.writes[:end])
}
