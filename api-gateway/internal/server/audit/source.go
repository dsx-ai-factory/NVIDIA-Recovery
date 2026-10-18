// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package audit provides the source adapter for API Gateway's audit facade.
package audit

import (
	"context"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
)

// AuditSource supplies audit ingestion and reads to the RPC server.
type AuditSource interface {
	// GetAuditEvents returns a page of persisted public audit events.
	GetAuditEvents(context.Context, *pb.GetAuditEventsRequest) (*pb.GetAuditEventsResponse, error)
	// CreateAuditEvents submits a producer batch and reports its acceptance.
	CreateAuditEvents(context.Context, *pb.CreateAuditEventsRequest) (*pb.CreateAuditEventsResponse, error)
}
