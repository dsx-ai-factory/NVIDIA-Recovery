// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/server/audit"
)

// auditServer implements AuditService using the configured audit source.
type auditServer struct {
	pb.UnimplementedAuditServiceServer
	auditSource audit.AuditSource
}

var _ pb.AuditServiceServer = (*auditServer)(nil)

// NewAuditServer constructs the audit RPC facade.
func NewAuditServer(auditSource audit.AuditSource) pb.AuditServiceServer {
	return &auditServer{auditSource: auditSource}
}

// CreateAuditEvents submits a producer batch through the configured audit source.
func (s *auditServer) CreateAuditEvents(ctx context.Context, request *pb.CreateAuditEventsRequest) (*pb.CreateAuditEventsResponse, error) {
	return s.auditSource.CreateAuditEvents(ctx, request)
}

// GetAuditEvents queries persisted events through the configured audit source.
func (s *auditServer) GetAuditEvents(ctx context.Context, request *pb.GetAuditEventsRequest) (*pb.GetAuditEventsResponse, error) {
	return s.auditSource.GetAuditEvents(ctx, request)
}
