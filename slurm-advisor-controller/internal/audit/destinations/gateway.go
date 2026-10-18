// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package destinations

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"time"

	"github.com/cockroachdb/errors"
	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	defaultGatewayMaxAttempts    = 5
	defaultGatewayInitialBackoff = 100 * time.Millisecond
	defaultGatewayMaxBackoff     = 2 * time.Second
)

// retryPolicy is stateless configuration; Deliver owns the per-call attempt
// counter so every batch starts with the initial delay.
type retryPolicy struct {
	maxAttempts  int
	initialDelay time.Duration
	maxDelay     time.Duration
	// sleep waits out the backoff delay but returns early with ctx.Err() if
	// ctx is canceled first, so a Stop deadline bounds retry backoff too.
	sleep          func(ctx context.Context, delay time.Duration) error
	randomizeDelay func(time.Duration) time.Duration
}

// sleepUnlessCanceled is the production retryPolicy.sleep implementation.
func sleepUnlessCanceled(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// auditIngestionClient keeps this producer independent of query RPCs added to
// the generated AuditServiceClient.
type auditIngestionClient interface {
	CreateAuditEvents(context.Context, *pb.CreateAuditEventsRequest, ...grpc.CallOption) (*pb.CreateAuditEventsResponse, error)
}

// Gateway sends audit event batches through an API Gateway connection.
// The caller owns the connection and must keep it open until the buffer drains.
type Gateway struct {
	client         auditIngestionClient
	logger         *zap.Logger
	requestTimeout time.Duration
	retryPolicy    retryPolicy
}

// NewGateway constructs an audit destination using the caller's gateway client
// and credentials. Each attempt has a ten-second deadline.
func NewGateway(client auditIngestionClient, logger *zap.Logger) *Gateway {
	return &Gateway{
		client:         client,
		logger:         logger,
		requestTimeout: 10 * time.Second,
		retryPolicy: retryPolicy{
			maxAttempts:    defaultGatewayMaxAttempts,
			initialDelay:   defaultGatewayInitialBackoff,
			maxDelay:       defaultGatewayMaxBackoff,
			sleep:          sleepUnlessCanceled,
			randomizeDelay: randomDelayUpTo,
		},
	}
}

// Deliver sends one batch and retries transient delivery failures. A populated
// partial-success response is terminal under the ingestion contract.
func (d *Gateway) Deliver(ctx context.Context, events []audit.ProducerEvent) (int, error) {
	request := &pb.CreateAuditEventsRequest{Items: make([]*pb.ProducerAuditEvent, len(events))}
	for i, event := range events {
		payload := &structpb.Struct{}
		if event.Payload != nil {
			// Preserve JSON-compatible producer values, including nested typed
			// structs and slices that structpb.NewStruct does not accept.
			body, err := json.Marshal(event.Payload)
			if err != nil {
				return 0, errors.Wrap(err, "encode audit event payload")
			}
			if err := payload.UnmarshalJSON(body); err != nil {
				return 0, errors.Wrap(err, "convert audit event payload to protobuf")
			}
		}
		request.Items[i] = &pb.ProducerAuditEvent{
			HeimdallJobId:   event.HeimdallJobID,
			EventProducer:   string(event.EventProducer),
			EventType:       string(event.EventType),
			EventLevel:      string(event.EventLevel),
			Payload:         payload,
			EventProducedAt: event.EventProducedAt,
			IdempotencyKey:  event.IdempotencyKey,
		}
	}

	for attempt := 0; ; attempt++ {
		attemptDelivered, retryable, err := d.deliverOnce(ctx, request)
		if err == nil {
			d.logger.Info("sent audit event batch", zap.Int("event_count", len(events)))
			return len(events), nil
		}
		if !retryable || attempt+1 >= d.retryPolicy.maxAttempts {
			return attemptDelivered, err
		}

		if sleepErr := d.retryPolicy.sleep(ctx, d.retryPolicy.delay(attempt)); sleepErr != nil {
			return attemptDelivered, sleepErr
		}
	}
}

func (d *Gateway) deliverOnce(ctx context.Context, request *pb.CreateAuditEventsRequest) (int, bool, error) {
	// Shutdown drains queued events after the process context is canceled. Give
	// each attempt its own deadline so that drain can finish while remaining bounded.
	ctx, cancel := context.WithTimeout(ctx, d.requestTimeout)
	defer cancel()

	result, err := d.client.CreateAuditEvents(ctx, request)
	if err != nil {
		code := status.Code(err)
		retryable := code == codes.Unavailable || code == codes.DeadlineExceeded || code == codes.ResourceExhausted
		return 0, retryable, errors.Wrap(err, "send audit event batch")
	}
	eventCount := len(request.Items)
	if result.PartialSuccess == nil {
		return eventCount, false, nil
	}

	rejected := int(result.PartialSuccess.RejectedEvents)
	if rejected <= 0 || rejected > eventCount {
		return 0, false, errors.Newf(
			"audit ingestion returned invalid rejected event count %d for batch size %d",
			rejected,
			eventCount,
		)
	}

	return eventCount - rejected, false, errors.Newf(
		"audit ingestion partially accepted batch: rejected %d of %d events",
		rejected,
		eventCount,
	)
}

func (p retryPolicy) delay(retryIndex int) time.Duration {
	delay := p.initialDelay
	for range retryIndex {
		if delay >= p.maxDelay/2 {
			delay = p.maxDelay
			break
		}
		delay *= 2
	}
	if delay > p.maxDelay {
		delay = p.maxDelay
	}
	return p.randomizeDelay(delay)
}

// randomDelayUpTo spreads retries uniformly from zero through the calculated
// exponential delay so multiple clients do not retry in lockstep.
func randomDelayUpTo(delay time.Duration) time.Duration {
	return time.Duration(rand.Int64N(int64(delay) + 1))
}

var _ audit.EventDestination[audit.ProducerEvent] = (*Gateway)(nil)
