// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const apiKeysDisabledMessage = "API key management is disabled"

type disabledApiKeysServer struct {
	pb.UnimplementedApiKeysServiceServer
}

var _ pb.ApiKeysServiceServer = (*disabledApiKeysServer)(nil)

func NewDisabledApiKeysServer() pb.ApiKeysServiceServer {
	return &disabledApiKeysServer{}
}

func (s *disabledApiKeysServer) CreateApiKey(context.Context, *pb.CreateApiKeyRequest) (*pb.CreateApiKeyResponse, error) {
	return nil, apiKeysDisabledError()
}

func (s *disabledApiKeysServer) RetrieveApiKey(context.Context, *pb.RetrieveApiKeyRequest) (*pb.RetrieveApiKeyResponse, error) {
	return nil, apiKeysDisabledError()
}

func (s *disabledApiKeysServer) UpdateApiKeyAuthorizations(context.Context, *pb.UpdateApiKeyAuthorizationsRequest) (*pb.UpdateApiKeyAuthorizationsResponse, error) {
	return nil, apiKeysDisabledError()
}

func (s *disabledApiKeysServer) DeleteApiKey(context.Context, *pb.DeleteApiKeyRequest) (*pb.DeleteApiKeyResponse, error) {
	return nil, apiKeysDisabledError()
}

func (s *disabledApiKeysServer) ListApiKeys(context.Context, *pb.ListApiKeysRequest) (*pb.ListApiKeysResponse, error) {
	return nil, apiKeysDisabledError()
}

func apiKeysDisabledError() error {
	return status.Error(codes.Internal, apiKeysDisabledMessage)
}
