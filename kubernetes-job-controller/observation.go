// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"slices"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/workload-map/karta/sdk/pkg/api/runai/v1alpha1"
	"github.com/dsx-ai-factory/workload-map/karta/sdk/pkg/catalog"
	"github.com/dsx-ai-factory/workload-map/karta/sdk/pkg/resource"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/structpb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func observation(ctx context.Context, w *unstructured.Unstructured, placement *placementIndex) (*pb.CreateOrUpdateJobRequest, error) {
	id := w.GetLabels()[jobIDLabel]
	parsed, err := uuid.Parse(id)
	if err != nil || parsed == uuid.Nil || parsed.String() != id || w.GetUID() == "" {
		return nil, errors.New("workload requires a stamped canonical job UUID and an object UID")
	}
	var attempt int32
	if raw, exists := w.GetLabels()[attemptLabel]; exists {
		index, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || index < 0 || strconv.FormatInt(index, 10) != raw {
			return nil, errors.New("workload has an invalid attempt index")
		}
		attempt = int32(index)
	}

	definition, err := catalog.Get(w.GroupVersionKind())
	if err != nil {
		return nil, errors.Wrap(err, "resolve Workload-Map definition")
	}
	root, err := resource.NewComponentFactoryFromObject(definition, w.DeepCopy()).GetRootComponent()
	if err != nil {
		return nil, errors.Wrap(err, "resolve Workload-Map root")
	}
	status, err := root.GetStatus(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "read workload status")
	}
	var matches []v1alpha1.ResourceStatus
	if status != nil {
		matches = status.MatchedStatuses
	}
	state, reason, err := classifyState(w, matches)
	if err != nil {
		return nil, err
	}

	fields := map[string]any{
		"scheduler": "kubernetes", "namespace": w.GetNamespace(), "job_name": w.GetName(),
		"job_api_version": w.GetAPIVersion(), "job_kind": w.GetKind(), "workload_uid": string(w.GetUID()),
	}
	if reason != "" {
		fields["termination_reason"] = reason
	}
	if created := w.GetCreationTimestamp(); !created.IsZero() {
		fields["kubernetes_submit_time"] = created.UTC().Format(time.RFC3339Nano)
	}
	metadata, err := structpb.NewStruct(fields)
	if err != nil {
		return nil, errors.Wrap(err, "encode scheduler metadata")
	}
	heimdallID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(id+"-"+strconv.FormatInt(int64(attempt), 10))).String()
	return &pb.CreateOrUpdateJobRequest{
		JobId: id, HeimdallJobId: &heimdallID, WorkflowId: id, AttemptIndex: attempt,
		JobName: new(w.GetName()), State: state, AllocatedNodes: placement.allocatedNodes(w, id, attempt), SchedulerMetadata: metadata,
	}, nil
}

// Native terminal conditions take precedence over overlapping normalized
// statuses. A suspended workload is pending publication of its next execution.
func classifyState(w *unstructured.Unstructured, matches []v1alpha1.ResourceStatus) (pb.JobStatus, string, error) {
	conditions, _, err := unstructured.NestedSlice(w.Object, "status", "conditions")
	if err != nil {
		return pb.JobStatus_JOB_STATUS_UNSPECIFIED, "", errors.Wrap(err, "read native conditions")
	}
	var completed bool
	var reason string
	for _, item := range conditions {
		condition, ok := item.(map[string]any)
		if !ok || condition["status"] != "True" {
			continue
		}
		switch condition["type"] {
		case "Failed":
			failure, _ := condition["reason"].(string)
			return pb.JobStatus_JOB_STATUS_FAILED, failure, nil
		case "Completed", "Succeeded":
			completed = true
			reason, _ = condition["reason"].(string)
		}
	}
	switch {
	case completed:
		return pb.JobStatus_JOB_STATUS_COMPLETED, reason, nil
	case slices.Contains(matches, v1alpha1.SuspendedStatus),
		slices.Contains(matches, v1alpha1.SuspendingStatus),
		slices.Contains(matches, v1alpha1.ResumingStatus):
		return pb.JobStatus_JOB_STATUS_PENDING, "", nil
	case slices.Contains(matches, v1alpha1.RunningStatus):
		return pb.JobStatus_JOB_STATUS_RUNNING, "", nil
	default:
		return pb.JobStatus_JOB_STATUS_PENDING, "", nil
	}
}
