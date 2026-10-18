// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"testing"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDisabledApiKeysServiceReturnsInternal(t *testing.T) {
	service := NewDisabledApiKeysServer()

	tests := []struct {
		name string
		call func(context.Context) error
	}{
		{
			name: "create",
			call: func(ctx context.Context) error {
				_, err := service.CreateApiKey(ctx, &pb.CreateApiKeyRequest{})
				return err
			},
		},
		{
			name: "retrieve",
			call: func(ctx context.Context) error {
				_, err := service.RetrieveApiKey(ctx, &pb.RetrieveApiKeyRequest{KeyId: "key_123"})
				return err
			},
		},
		{
			name: "update authorizations",
			call: func(ctx context.Context) error {
				_, err := service.UpdateApiKeyAuthorizations(ctx, &pb.UpdateApiKeyAuthorizationsRequest{KeyId: "key_123"})
				return err
			},
		},
		{
			name: "delete",
			call: func(ctx context.Context) error {
				_, err := service.DeleteApiKey(ctx, &pb.DeleteApiKeyRequest{KeyId: "key_123"})
				return err
			},
		},
		{
			name: "list",
			call: func(ctx context.Context) error {
				_, err := service.ListApiKeys(ctx, &pb.ListApiKeysRequest{})
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.call(context.Background())
			if status.Code(err) != codes.Internal {
				t.Fatalf("status code = %s, want %s", status.Code(err), codes.Internal)
			}
		})
	}
}
