// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package heimdall

import (
	"context"

	"github.com/cockroachdb/errors"
	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/grpcauth"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/tlsconfig"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type GRPCClient struct {
	client      pb.HeimdallServiceClient
	auditClient pb.AuditServiceClient
}

func NewGRPCClientFromConn(conn grpc.ClientConnInterface) *GRPCClient {
	return &GRPCClient{client: pb.NewHeimdallServiceClient(conn), auditClient: pb.NewAuditServiceClient(conn)}
}

// DialGRPCClient starts connecting asynchronously and logs once when the gateway
// connection reaches Ready. The returned cleanup stops the readiness watcher
// before returning; ctx cancellation also stops it without closing the client.
func DialGRPCClient(ctx context.Context, endpoint, serviceAccountTokenPath string, tlsConfig tlsconfig.Config, logger *zap.Logger) (*GRPCClient, func() error, error) {
	return dialGRPCClient(ctx, endpoint, serviceAccountTokenPath, tlsConfig, logger)
}

func dialGRPCClient(ctx context.Context, endpoint, serviceAccountTokenPath string, tlsConfig tlsconfig.Config, logger *zap.Logger, extraDialOptions ...grpc.DialOption) (*GRPCClient, func() error, error) {
	dialOptions, err := grpcauth.DialOptions(tlsConfig, serviceAccountTokenPath)
	if err != nil {
		return nil, nil, err
	}
	dialOptions = append(dialOptions, extraDialOptions...)

	conn, err := grpc.NewClient(endpoint, dialOptions...)
	if err != nil {
		return nil, nil, err
	}

	watchCtx, stopWatch := context.WithCancel(ctx)
	watchDone := make(chan struct{})
	// NewClient is lazy. Connect starts transport setup without an application RPC.
	conn.Connect()
	go func() {
		defer close(watchDone)
		logGatewayConnectionReady(watchCtx, conn, endpoint, logger)
	}()

	return NewGRPCClientFromConn(conn), func() error {
		stopWatch()
		err := conn.Close()
		// The connection owner joins the watcher so no log can race logger cleanup.
		<-watchDone
		return err
	}, nil
}

// logGatewayConnectionReady observes state changes until the first Ready state,
// cancellation, or shutdown. It confirms transport readiness, not RPC success.
func logGatewayConnectionReady(ctx context.Context, conn *grpc.ClientConn, endpoint string, logger *zap.Logger) {
	for ctx.Err() == nil {
		state := conn.GetState()
		if state == connectivity.Ready {
			logger.Info("API Gateway connection established", zap.String("heimdall_endpoint", endpoint))
			return
		}
		if state == connectivity.Shutdown || !conn.WaitForStateChange(ctx, state) {
			return
		}
	}
}

func (c *GRPCClient) CreateOrUpdateJob(ctx context.Context, req *pb.CreateOrUpdateJobRequest) (*pb.CreateOrUpdateJobResponse, error) {
	return c.client.CreateOrUpdateJob(ctx, req)
}

// GetJobs bootstraps non-final attempts and, when requested, recent failures.
// Separate paginated queries keep the end-time cutoff off long-running jobs.
// Both reads must finish before returning; a partial bootstrap is retried.
func (c *GRPCClient) GetJobs(ctx context.Context, opts ...PaginationOption) ([]*pb.Job, error) {
	pagination := ResolvePaginationOptions(opts...)
	queries := []*pb.GetJobsRequest{{CompletedJobs: new(false)}}
	if pagination.RecentFailuresSince != nil {
		queries = append(queries, &pb.GetJobsRequest{
			ReconciliationEligibleJobs: new(true),
			EndTimeFrom:                timestamppb.New(*pagination.RecentFailuresSince),
		})
	}
	var out []*pb.Job
	for _, req := range queries {
		req.Limit = pagination.PageLimit
		pageToken := ""
		for {
			req.PageToken = &pageToken
			resp, err := c.client.GetJobs(ctx, req)
			if err != nil {
				return nil, errors.Wrapf(err, "get jobs page %q", pageToken)
			}
			out = append(out, resp.GetJobs()...)
			pageToken = resp.GetNextPageToken()
			if pageToken == "" {
				break
			}
		}
	}
	return out, nil
}

func (c *GRPCClient) ListAdvice(ctx context.Context, opts ...PaginationOption) ([]*pb.Advice, error) {
	pagination := ResolvePaginationOptions(opts...)
	var out []*pb.Advice
	pageToken := ""
	for {
		resp, err := c.listAdvicePage(ctx, pageToken, pagination)
		if err != nil {
			return nil, errors.Wrapf(err, "list advice page %q", pageToken)
		}
		if resp != nil {
			out = append(out, resp.GetAdvice()...)
			pageToken = resp.GetNextPageToken()
		} else {
			pageToken = ""
		}
		if pageToken == "" {
			return out, nil
		}
	}
}

func (c *GRPCClient) listAdvicePage(ctx context.Context, pageToken string, pagination PaginationOptions) (*pb.ListAdviceResponse, error) {
	req := &pb.ListAdviceRequest{Page: &pageToken, Limit: pagination.PageLimit}
	if pagination.OnlyActive {
		req.OnlyActive = &pagination.OnlyActive
	}

	return c.client.ListAdvice(ctx, req)
}

func (c *GRPCClient) UpdateAdviceReconciliationStatus(
	ctx context.Context,
	adviceID string,
	status *pb.AdviceReconciliationStatus,
) (*pb.AdviceReconciliationStatus, error) {
	resp, err := c.client.UpdateAdviceReconciliationStatus(ctx, &pb.UpdateAdviceReconciliationStatusRequest{
		HeimdallJobId: adviceID,
		Status:        status,
	})
	if err != nil {
		return nil, err
	}

	return resp.GetStatus(), nil
}

// CreateAuditEvents delivers a batch on the same connection and credentials as
// Job Manager calls.
func (c *GRPCClient) CreateAuditEvents(ctx context.Context, req *pb.CreateAuditEventsRequest, opts ...grpc.CallOption) (*pb.CreateAuditEventsResponse, error) {
	return c.auditClient.CreateAuditEvents(ctx, req, opts...)
}
