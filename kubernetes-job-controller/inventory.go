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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// inventoryPageSize is the gateway's maximum GetJobs page size.
const inventoryPageSize = 250

// inventoryBackoff retries the startup scan until the gateway answers or the
// process stops. Publication cannot start without the restart inventory.
var inventoryBackoff = wait.Backoff{Duration: time.Second, Factor: 2, Jitter: 0.1, Steps: 8, Cap: time.Minute}

// openAttempts maps each served kind to the open backend attempts this
// publisher is responsible for, keyed by workload name and attempt identity.
type openAttempts map[schema.GroupVersionKind]attempts

// loadOpenAttempts reads non-final backend attempts once at startup. Job
// Manager rows are the durable restart inventory: they identify workload
// objects that may have been deleted or replaced while the controller was
// down. A failed page restarts the scan after backoff.
func loadOpenAttempts(ctx context.Context, backend jobBackend, namespace string, kinds []schema.GroupVersionKind, logger *zap.Logger) (openAttempts, error) {
	var inventory openAttempts
	err := inventoryBackoff.DelayFunc().Until(ctx, true, true, func(ctx context.Context) (bool, error) {
		var err error
		inventory, err = scanOpenAttempts(ctx, backend, namespace, kinds)
		if err != nil {
			logger.Warn("retrying open attempt inventory", zap.Error(err))
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "read open attempt inventory")
	}
	return inventory, nil
}

// scanOpenAttempts pages through every non-final attempt and keeps the rows
// this publisher owns.
func scanOpenAttempts(ctx context.Context, backend jobBackend, namespace string, kinds []schema.GroupVersionKind) (openAttempts, error) {
	inventory := make(openAttempts)
	request := &pb.GetJobsRequest{Limit: inventoryPageSize, CompletedJobs: new(false)}
	for {
		callCtx, cancel := context.WithTimeout(ctx, backendCallTimeout)
		response, err := backend.GetJobs(callCtx, request)
		cancel()
		if err != nil {
			return nil, errors.Wrap(err, "list open attempts")
		}
		for _, job := range response.GetJobs() {
			kind, name, tracked, ok := inventoryAttempt(job, namespace, kinds)
			if !ok {
				continue
			}
			if inventory[kind] == nil {
				inventory[kind] = make(attempts)
			}
			inventory[kind].put(name, tracked)
		}
		next := response.GetNextPageToken()
		if next == "" || len(response.GetJobs()) == 0 {
			return inventory, nil
		}
		request.PageToken = new(next)
	}
}

// inventoryAttempt recognizes a direct Kubernetes observation for an open
// attempt of a served kind in this publisher's namespace.
func inventoryAttempt(job *pb.Job, namespace string, kinds []schema.GroupVersionKind) (schema.GroupVersionKind, types.NamespacedName, *trackedAttempt, bool) {
	fields := job.GetSchedulerMetadata().GetFields()
	text := func(key string) string { return fields[key].GetStringValue() }
	gv, err := schema.ParseGroupVersion(text("job_api_version"))
	kind := gv.WithKind(text("job_kind"))
	switch {
	case text("scheduler") != "kubernetes", text("workload_uid") == "", text("namespace") != namespace,
		text("job_name") == "", err != nil, !slices.Contains(kinds, kind), !isOpen(job.GetState()):
		return schema.GroupVersionKind{}, types.NamespacedName{}, nil, false
	}
	return kind, types.NamespacedName{Namespace: namespace, Name: text("job_name")}, &trackedAttempt{
		uid: types.UID(text("workload_uid")),
		last: &pb.CreateOrUpdateJobRequest{
			HeimdallJobId: new(job.GetHeimdallJobId()), JobId: job.GetJobId(), WorkflowId: job.GetWorkflowId(),
			AttemptIndex: job.GetAttemptIndex(), JobName: job.JobName, State: job.GetState(),
			AllocatedNodes: job.GetNodes(), SchedulerMetadata: job.GetSchedulerMetadata(),
		},
		confirmed: true,
	}, true
}

// seedSource enqueues inventory names when the controller starts. It performs
// no I/O; reconcile resolves each name against the cache and live API.
func seedSource(names []types.NamespacedName) source.Source {
	return source.Func(func(_ context.Context, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
		for _, name := range names {
			queue.Add(reconcile.Request{NamespacedName: name})
		}
		return nil
	})
}
