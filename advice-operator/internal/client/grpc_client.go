// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"crypto/tls"
	"net/url"
	"time"

	"github.com/cockroachdb/errors"
	heimdallv1 "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultGRPCTimeout = 30 * time.Second
)

// GRPCClientConfig contains configuration options for the gRPC client.
type GRPCClientConfig struct {
	// Address is the target address of the Heimdall gRPC server (e.g., "localhost:50051").
	Address string
	// UseTLS enables TLS for the connection.
	UseTLS bool
	// InsecureSkipVerify skips TLS certificate verification (for development only).
	InsecureSkipVerify bool
	// Timeout for RPC calls.
	Timeout time.Duration
	// Logger records client lifecycle information.
	Logger *zap.Logger
}

// HeimdallGRPCClient is a gRPC client for the Heimdall service.
type HeimdallGRPCClient struct {
	conn    *grpc.ClientConn
	client  heimdallv1.HeimdallServiceClient
	timeout time.Duration
	logger  *zap.Logger
}

// Ensure HeimdallGRPCClient implements AdviceSource at compile time.
var _ AdviceSource = (*HeimdallGRPCClient)(nil)

// NewHeimdallGRPCClient creates a new Heimdall gRPC client.
func NewHeimdallGRPCClient(ctx context.Context, cfg GRPCClientConfig) (*HeimdallGRPCClient, error) {
	var dialOpts []grpc.DialOption

	if cfg.UseTLS {
		tlsConfig := &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: cfg.InsecureSkipVerify,
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Parse the address to extract host:port from URL if needed
	address := cfg.Address
	if parsedURL, err := url.Parse(cfg.Address); err == nil && parsedURL.Host != "" {
		// If it's a valid URL with a host, use the host:port
		address = parsedURL.Host
	}

	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("heimdall-grpc-client")
	logger.Debug("connecting to Heimdall gRPC server", zap.String("address", address))

	conn, err := grpc.NewClient(address, dialOpts...)
	if err != nil {
		return nil, errors.Wrap(err, "connect to Heimdall gRPC server")
	}

	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = defaultGRPCTimeout
	}

	return &HeimdallGRPCClient{
		conn:    conn,
		client:  heimdallv1.NewHeimdallServiceClient(conn),
		timeout: timeout,
		logger:  logger,
	}, nil
}

// Close closes the gRPC connection.
func (c *HeimdallGRPCClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// ListAdvice fetches all advice from the Heimdall gRPC API.
func (c *HeimdallGRPCClient) ListAdvice(ctx context.Context) ([]APIAdvice, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	c.logger.Debug("listing advice via gRPC")

	var allAdvice []APIAdvice
	var pageToken *string

	for {
		req := &heimdallv1.ListAdviceRequest{
			Page:  pageToken,
			Limit: 100, // Max page size
		}

		resp, err := c.client.ListAdvice(ctx, req)
		if err != nil {
			return nil, errors.Wrap(err, "list advice")
		}

		// Convert proto advice to API advice
		for _, protoAdvice := range resp.GetAdvice() {
			apiAdvice := convertProtoToAPIAdvice(protoAdvice)
			allAdvice = append(allAdvice, apiAdvice)
		}

		// Check for more pages
		nextToken := resp.GetNextPageToken()
		if nextToken == "" {
			break
		}
		pageToken = &nextToken
	}

	c.logger.Debug("listed advice", zap.Int("count", len(allAdvice)))
	return allAdvice, nil
}

// convertProtoToAPIAdvice converts a proto Advice message to an APIAdvice struct.
func convertProtoToAPIAdvice(proto *heimdallv1.Advice) APIAdvice {
	advice := APIAdvice{
		HeimdallID:   proto.GetHeimdallJobId(),
		JobID:        proto.GetJobId(),
		AttemptIndex: proto.GetAttemptIndex(),
	}

	// Convert scheduler metadata (protobuf Struct to map[string]interface{})
	if schedulerMeta := proto.GetSchedulerMetadata(); schedulerMeta != nil {
		advice.SchedulerMetadata = schedulerMeta.AsMap()
	}

	// Convert advice recommendation
	if protoRec := proto.GetAdvice(); protoRec != nil {
		advice.Advice = convertProtoAdviceRecommendation(protoRec)
	}

	// Convert issues
	if protoIssues := proto.GetIssues(); len(protoIssues) > 0 {
		advice.Issues = make([]APIIssue, len(protoIssues))
		for i, protoIssue := range protoIssues {
			advice.Issues[i] = convertProtoIssue(protoIssue)
		}
	}

	// Convert timestamps
	if createdAt := proto.GetCreatedAt(); createdAt != nil {
		advice.CreatedAt = createdAt.AsTime().Format(time.RFC3339)
	}
	if updatedAt := proto.GetUpdatedAt(); updatedAt != nil {
		advice.UpdatedAt = updatedAt.AsTime().Format(time.RFC3339)
	}

	return advice
}

// convertProtoAdviceRecommendation converts a proto AdviceRecommendation to an APIAdviceRecommendation.
func convertProtoAdviceRecommendation(proto *heimdallv1.AdviceRecommendation) APIAdviceRecommendation {
	rec := APIAdviceRecommendation{}

	// Convert state
	state := proto.GetState()
	if state != heimdallv1.AdviceState_ADVICE_STATE_UNSPECIFIED {
		stateStr := state.String()
		rec.State = &stateStr
	}

	// Convert node constraints
	if constraints := proto.GetNodeConstraints(); constraints != nil {
		rec.NodeConstraints = &APINodeConstraints{
			Exclusions: constraints.GetExclusions(),
			Inclusions: constraints.GetInclusions(),
		}
	}

	return rec
}

// convertProtoIssue converts a proto Issue to an APIIssue.
func convertProtoIssue(proto *heimdallv1.Issue) APIIssue {
	issue := APIIssue{
		IssueType:        proto.GetIssueType(),
		DetectedBy:       sanitizeDetectedBy(proto.GetDetectedBy()),
		RelevantLogLines: proto.GetRelevantLogLines(),
	}

	// Convert timestamp (now a protobuf Timestamp instead of string)
	if ts := proto.GetTimestamp(); ts != nil {
		issue.Timestamp = ts.AsTime().Format(time.RFC3339)
	}

	// Convert remediation actions
	if protoActions := proto.GetRemediationActions(); len(protoActions) > 0 {
		issue.RemediationActions = make([]APIRemediationAction, len(protoActions))
		for i, protoAction := range protoActions {
			issue.RemediationActions[i] = convertProtoRemediationAction(protoAction)
		}
	}

	return issue
}

// convertProtoRemediationAction converts a proto RemediationAction to an APIRemediationAction.
func convertProtoRemediationAction(proto *heimdallv1.RemediationAction) APIRemediationAction {
	action := APIRemediationAction{
		ActionType: proto.GetActionType(),
		Reason:     proto.GetReason(),
	}

	// Convert timestamp (now a protobuf Timestamp instead of string)
	if ts := proto.GetTimestamp(); ts != nil {
		action.Timestamp = ts.AsTime().Format(time.RFC3339)
	}

	// Convert resource details
	if protoDetails := proto.GetResourceDetails(); len(protoDetails) > 0 {
		action.ResourceDetails = make([]APIResourceDetail, len(protoDetails))
		for i, protoDetail := range protoDetails {
			action.ResourceDetails[i] = APIResourceDetail{
				ResourceType: protoDetail.GetResourceType(),
				ResourceID:   protoDetail.GetResourceId(),
				Details:      protoDetail.GetDetails(),
			}
		}
	}

	return action
}
