// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package audit

import (
	"context"
	"net/http"
	"net/url"

	"github.com/cockroachdb/errors"
	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	notificationsbindings "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/notifications"
	auditclient "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/audit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// AuditClientSource adapts the downstream audit HTTP client to AuditSource.
type AuditClientSource struct {
	client *auditclient.Client
}

var _ AuditSource = (*AuditClientSource)(nil)

// NewAuditClientSource constructs an HTTP-backed source for audit ingestion and reads.
func NewAuditClientSource(endpoint string, httpClient *http.Client) (*AuditClientSource, error) {
	client, err := auditclient.NewClient(endpoint, httpClient)
	if err != nil {
		return nil, err
	}
	return &AuditClientSource{client: client}, nil
}

// CreateAuditEvents translates a producer batch and its downstream acceptance.
func (s *AuditClientSource) CreateAuditEvents(ctx context.Context, request *pb.CreateAuditEventsRequest) (*pb.CreateAuditEventsResponse, error) {
	response, err := s.client.CreateAuditEvents(ctx, toNotificationsRequest(request))
	if err != nil {
		return nil, downstreamErrorToStatus(err)
	}
	return toProtoResponse(response), nil
}

func toNotificationsRequest(request *pb.CreateAuditEventsRequest) notificationsbindings.ProducerAuditEventBatch {
	events := request.GetItems()
	items := make([]notificationsbindings.ProducerAuditEvent, len(events))
	for index, event := range events {
		var payload map[string]any
		if event.GetPayload() != nil {
			payload = event.GetPayload().AsMap()
		}
		items[index] = notificationsbindings.ProducerAuditEvent{
			HeimdallJobId:   event.GetHeimdallJobId(),
			EventProducer:   event.GetEventProducer(),
			EventType:       event.GetEventType(),
			EventLevel:      notificationsbindings.AuditEventLevel(event.GetEventLevel()),
			Payload:         payload,
			EventProducedAt: event.GetEventProducedAt(),
			IdempotencyKey:  event.GetIdempotencyKey(),
		}
	}
	return notificationsbindings.ProducerAuditEventBatch{Items: items}
}

func toProtoResponse(response *notificationsbindings.AuditEventIngestionResponse) *pb.CreateAuditEventsResponse {
	result := &pb.CreateAuditEventsResponse{}
	if partialSuccess, ok := response.GetPartialSuccessOk(); ok && partialSuccess != nil {
		result.PartialSuccess = &pb.AuditEventPartialSuccess{
			RejectedEvents: partialSuccess.RejectedEvents,
			ErrorMessage:   partialSuccess.ErrorMessage,
		}
	}
	return result
}

func downstreamErrorToStatus(err error) error {
	if errors.Is(err, auditclient.ErrResponseTooLarge) {
		return status.Error(codes.ResourceExhausted, "downstream audit response exceeds size limit")
	}
	var httpError *auditclient.HTTPError
	if errors.As(err, &httpError) {
		return status.Error(downstreamStatusCode(httpError.StatusCode), httpError.Error())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "downstream audit timed out")
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "downstream audit was canceled")
	}
	var urlError *url.Error
	if errors.As(err, &urlError) {
		return status.Error(codes.Unavailable, "downstream audit is unavailable")
	}
	return status.Error(codes.Internal, "downstream audit failed")
}

func downstreamStatusCode(statusCode int) codes.Code {
	switch statusCode {
	case http.StatusBadRequest, http.StatusRequestEntityTooLarge, http.StatusUnprocessableEntity:
		return codes.InvalidArgument
	case http.StatusNotFound:
		return codes.NotFound
	case http.StatusRequestTimeout:
		return codes.DeadlineExceeded
	case http.StatusTooEarly, http.StatusTooManyRequests:
		return codes.ResourceExhausted
	default:
		if statusCode >= http.StatusInternalServerError {
			return codes.Unavailable
		}
		return codes.FailedPrecondition
	}
}

// GetAuditEvents translates filters and a bounded page without exposing storage-only fields.
func (s *AuditClientSource) GetAuditEvents(ctx context.Context, request *pb.GetAuditEventsRequest) (*pb.GetAuditEventsResponse, error) {
	if request == nil {
		request = &pb.GetAuditEventsRequest{}
	}
	if request.AttemptIndex != nil && (request.JobId == nil || request.GetAttemptIndex() < 0) {
		return nil, status.Error(codes.InvalidArgument, "attempt_index requires job_id and must be non-negative")
	}
	if request.HeimdallJobId != nil && (request.JobId != nil || request.AttemptIndex != nil) {
		return nil, status.Error(codes.InvalidArgument, "heimdall_job_id conflicts with scheduler job selectors")
	}
	if request.Limit != nil && (request.GetLimit() < 1 || request.GetLimit() > 100) {
		return nil, status.Error(codes.InvalidArgument, "limit must be between 1 and 100")
	}
	response, err := s.client.GetAuditEvents(ctx, func(query notificationsbindings.ApiAuditEventsGetRequest) notificationsbindings.ApiAuditEventsGetRequest {
		if request.HeimdallJobId != nil {
			query = query.HeimdallJobId(request.GetHeimdallJobId())
		}
		if request.JobId != nil {
			query = query.JobId(request.GetJobId())
		}
		if request.AttemptIndex != nil {
			query = query.AttemptIndex(request.GetAttemptIndex())
		}
		if request.EventType != nil {
			query = query.EventType(request.GetEventType())
		}
		if request.EventLevel != nil {
			query = query.EventLevel(notificationsbindings.AuditEventLevel(request.GetEventLevel()))
		}
		if request.EventProducer != nil {
			query = query.EventProducer(request.GetEventProducer())
		}
		if request.MinEventProducedAt != nil {
			query = query.MinEventProducedAt(request.GetMinEventProducedAt())
		}
		if request.MaxEventProducedAt != nil {
			query = query.MaxEventProducedAt(request.GetMaxEventProducedAt())
		}
		if request.SortBy != nil {
			query = query.SortBy(request.GetSortBy())
		}
		if request.Cursor != nil {
			query = query.Cursor(request.GetCursor())
		}
		if request.Limit != nil {
			query = query.Limit(request.GetLimit())
		}
		return query
	})
	if err != nil {
		return nil, downstreamErrorToStatus(err)
	}
	result := &pb.GetAuditEventsResponse{
		Cursor: &pb.AuditEventPageCursor{PrevCursor: response.Cursor.GetPrevCursor(), NextCursor: response.Cursor.GetNextCursor()},
		Items:  make([]*pb.AuditEvent, 0, len(response.Items)),
	}
	for _, event := range response.Items {
		payload, err := structpb.NewStruct(event.Payload)
		if err != nil {
			return nil, status.Error(codes.Internal, "downstream audit payload is invalid")
		}
		result.Items = append(result.Items, &pb.AuditEvent{
			HeimdallJobId: event.HeimdallJobId, EventProducer: event.EventProducer,
			EventType: event.EventType, EventLevel: string(event.EventLevel),
			Payload: payload, EventProducedAt: event.EventProducedAt,
			Summary: &pb.AuditEventHumanSummary{
				Title: event.Summary.Title, Description: event.Summary.Description,
				Granularity: int32(event.Summary.Granularity),
			},
		})
	}
	return result, nil
}
