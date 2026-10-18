// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package jobmanager

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	jobmanagerBindings "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/jobmanager"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUpsertJobMapsDownstreamInputErrorsToInvalidArgument(t *testing.T) {
	t.Parallel()

	const sensitiveDetail = "customer-secret-path"
	for _, statusCode := range []int{
		http.StatusBadRequest,
		http.StatusUnprocessableEntity,
	} {
		t.Run(http.StatusText(statusCode), func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(statusCode)
				_, _ = writer.Write([]byte(`{"detail":"` + sensitiveDetail + `"}`))
			}))
			defer server.Close()

			input := jobmanagerBindings.NewSlurmJobExecutionStatusInput("12345", 0, "workflow")
			_, err := NewJobManagerClient(server.URL).UpsertJob(context.Background(), input)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("UpsertJob() code = %v, want %v (error: %v)", status.Code(err), codes.InvalidArgument, err)
			}
			if got, want := status.Convert(err).Message(), "job manager rejected job input"; got != want {
				t.Fatalf("UpsertJob() message = %q, want %q", got, want)
			}
			if strings.Contains(err.Error(), sensitiveDetail) {
				t.Fatalf("UpsertJob() error leaks downstream response detail: %v", err)
			}
		})
	}
}

func TestUpsertJobLeavesOtherDownstreamErrorsUnchanged(t *testing.T) {
	t.Parallel()

	const responseBody = `{"detail":"internal failure"}`
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusInternalServerError)
		_, _ = writer.Write([]byte(responseBody))
	}))
	defer server.Close()

	input := jobmanagerBindings.NewSlurmJobExecutionStatusInput("12345", 0, "workflow")
	_, err := NewJobManagerClient(server.URL).UpsertJob(context.Background(), input)
	if err == nil {
		t.Fatal("UpsertJob() error = nil, want downstream error")
	}
	if status.Code(err) == codes.InvalidArgument {
		t.Fatalf("UpsertJob() error = %v, want downstream error unchanged", err)
	}
	var downstreamError *jobmanagerBindings.GenericOpenAPIError
	if !errors.As(err, &downstreamError) {
		t.Fatalf("UpsertJob() error type = %T, want *jobmanager.GenericOpenAPIError", err)
	}
	if got := string(downstreamError.Body()); got != responseBody {
		t.Fatalf("UpsertJob() downstream body = %q, want %q", got, responseBody)
	}
}
