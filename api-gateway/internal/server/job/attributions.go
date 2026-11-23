// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package job

import (
	"context"
	"time"

	v1 "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	jobmanagerBindings "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/jobmanager"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/jobmanager"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// parseOptionalTimestamp parses an optional ISO-8601 timestamp filter into a
// *time.Time, returning an InvalidArgument error when the value is malformed.
func parseOptionalTimestamp(value *string, field string) (*time.Time, error) {
	if value == nil || *value == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339, *value)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s must be an RFC3339 timestamp: %v", field, err)
	}
	return &parsed, nil
}

func nullableTimeToPb(nt jobmanagerBindings.NullableTime) *timestamppb.Timestamp {
	if nt.IsSet() && nt.Get() != nil {
		return timestamppb.New(*nt.Get())
	}
	return nil
}

func nullableStringToPtr(ns jobmanagerBindings.NullableString) *string {
	if ns.IsSet() {
		return ns.Get()
	}
	return nil
}

// mapExclusionStatusValueToProto maps a job manager exclusion status to proto.
func mapExclusionStatusValueToProto(s jobmanagerBindings.ExclusionApplicationStatus) v1.ExclusionApplicationStatus {
	switch s {
	case jobmanagerBindings.EXCLUSIONAPPLICATIONSTATUS_UNKNOWN:
		return v1.ExclusionApplicationStatus_EXCLUSION_APPLICATION_STATUS_UNKNOWN
	case jobmanagerBindings.EXCLUSIONAPPLICATIONSTATUS_NOT_APPLICABLE:
		return v1.ExclusionApplicationStatus_EXCLUSION_APPLICATION_STATUS_NOT_APPLICABLE
	case jobmanagerBindings.EXCLUSIONAPPLICATIONSTATUS_NOT_APPLIED:
		return v1.ExclusionApplicationStatus_EXCLUSION_APPLICATION_STATUS_NOT_APPLIED
	case jobmanagerBindings.EXCLUSIONAPPLICATIONSTATUS_APPLIED:
		return v1.ExclusionApplicationStatus_EXCLUSION_APPLICATION_STATUS_APPLIED
	default:
		return v1.ExclusionApplicationStatus_EXCLUSION_APPLICATION_STATUS_UNSPECIFIED
	}
}

func transformJobExecutionErrorToProto(e jobmanagerBindings.JobExecutionError) *v1.JobExecutionError {
	return &v1.JobExecutionError{
		Code:        e.Code,
		Message:     e.Message,
		Description: e.Description,
		Signature:   e.Signature,
	}
}

func transformNodeEventToProto(e *jobmanagerBindings.NodeEvent) *v1.NodeEvent {
	pb := &v1.NodeEvent{
		Node:                    e.Node,
		Timestamp:               timestamppb.New(e.Timestamp),
		JobId:                   e.JobId,
		JobAttemptIndex:         e.JobAttemptIndex,
		JobStartTime:            nullableTimeToPb(e.JobStartTime),
		JobEndTime:              nullableTimeToPb(e.JobEndTime),
		JobExecutionError:       transformJobExecutionErrorToProto(e.JobExecutionError),
		Cluster:                 nullableStringToPtr(e.Cluster),
		ErrorAttribution:        e.ErrorAttribution,
		ErrorAttributionType:    e.ErrorAttributionType,
		ErrorAttributionOrigin:  e.ErrorAttributionOrigin,
		ErrorAttributionSources: e.ErrorAttributionSources,
		ExclusionType:           e.ExclusionType,
		OriginalExclusionType:   nullableStringToPtr(e.OriginalExclusionType),
		FactStatus:              e.FactStatus,
	}
	if e.ExclusionApplied != nil {
		applied := mapExclusionStatusValueToProto(*e.ExclusionApplied)
		pb.ExclusionApplied = &applied
	}
	return pb
}

func transformGlobalEventToProto(e *jobmanagerBindings.GlobalEvent) *v1.GlobalEvent {
	return &v1.GlobalEvent{
		JobId:                   e.JobId,
		JobAttemptIndex:         e.JobAttemptIndex,
		Timestamp:               timestamppb.New(e.Timestamp),
		JobExecutionError:       transformJobExecutionErrorToProto(e.JobExecutionError),
		ErrorAttribution:        e.ErrorAttribution,
		ErrorAttributionType:    e.ErrorAttributionType,
		ErrorAttributionOrigin:  e.ErrorAttributionOrigin,
		ErrorAttributionSources: e.ErrorAttributionSources,
		FactStatus:              e.FactStatus,
	}
}

func transformNodeEventsToProto(ne *jobmanagerBindings.NodeEvents) *v1.NodeEvents {
	if ne == nil {
		return nil
	}
	pb := &v1.NodeEvents{
		Count:              ne.ItemCount,
		AttributionSummary: ne.AttributionSummary,
		AttributionInsight: ne.AttributionInsight,
	}
	pb.Events = make([]*v1.NodeEvent, 0, len(ne.Events))
	for i := range ne.Events {
		pb.Events = append(pb.Events, transformNodeEventToProto(&ne.Events[i]))
	}
	if len(ne.GlobalEvents) > 0 {
		pb.GlobalEvents = make([]*v1.GlobalEvent, 0, len(ne.GlobalEvents))
		for i := range ne.GlobalEvents {
			pb.GlobalEvents = append(pb.GlobalEvents, transformGlobalEventToProto(&ne.GlobalEvents[i]))
		}
	}
	return pb
}

// GetErrorAttributions implements JobSource.
func (j *JobClientSource) GetErrorAttributions(ctx context.Context, req *v1.GetErrorAttributionsRequest) (*v1.GetErrorAttributionsResponse, error) {
	startTimestamp, err := parseOptionalTimestamp(req.StartTimestamp, "start_timestamp")
	if err != nil {
		return nil, err
	}
	endTimestamp, err := parseOptionalTimestamp(req.EndTimestamp, "end_timestamp")
	if err != nil {
		return nil, err
	}

	attributions, err := j.jobManager.GetErrorAttributions(ctx, jobmanager.ErrorAttributionsParams{
		NodeNames:      req.NodeNames,
		JobIds:         req.JobIds,
		StartTimestamp: startTimestamp,
		EndTimestamp:   endTimestamp,
		AnomalyUuid:    req.AnomalyUuid,
		Limit:          req.Limit,
		Offset:         req.Offset,
	})
	if err != nil {
		return nil, err
	}

	return &v1.GetErrorAttributionsResponse{
		Attributions: transformNodeEventsToProto(attributions),
	}, nil
}

// GetJobErrorAttributions implements JobSource.
func (j *JobClientSource) GetJobErrorAttributions(ctx context.Context, req *v1.GetJobErrorAttributionsRequest) (*v1.GetJobErrorAttributionsResponse, error) {
	attributions, err := j.jobManager.GetJobErrorAttributions(ctx, jobmanager.JobErrorAttributionsParams{
		JobId:           req.JobId,
		JobAttemptIndex: req.JobAttemptIndex,
		AnomalyUuid:     req.AnomalyUuid,
	})
	if err != nil {
		return nil, err
	}

	return &v1.GetJobErrorAttributionsResponse{
		Attributions: transformNodeEventsToProto(attributions),
	}, nil
}
