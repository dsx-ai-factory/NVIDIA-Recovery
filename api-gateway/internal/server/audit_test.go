// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"testing"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
)

func TestCreateAuditEventsForwardsRequestAndResponse(t *testing.T) {
	request := &pb.CreateAuditEventsRequest{}
	want := &pb.CreateAuditEventsResponse{PartialSuccess: &pb.AuditEventPartialSuccess{RejectedEvents: 1}}
	ingestor := &recordingIngestor{response: want}

	got, err := NewAuditServer(ingestor).CreateAuditEvents(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateAuditEvents() failed: %v", err)
	}
	if ingestor.request != request {
		t.Fatal("CreateAuditEvents() did not forward the request")
	}
	if got != want {
		t.Fatalf("response = %#v, want %#v", got, want)
	}
}

type recordingIngestor struct {
	pb.UnimplementedAuditServiceServer
	request  *pb.CreateAuditEventsRequest
	response *pb.CreateAuditEventsResponse
	err      error
}

func (ingestor *recordingIngestor) CreateAuditEvents(_ context.Context, request *pb.CreateAuditEventsRequest) (*pb.CreateAuditEventsResponse, error) {
	ingestor.request = request
	return ingestor.response, ingestor.err
}
