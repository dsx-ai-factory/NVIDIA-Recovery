// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"testing"
	"time"

	heimdallv1 "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestConvertProtoToAPIAdviceSanitizesEmptyDetectedByForCRD(t *testing.T) {
	advice := convertProtoToAPIAdvice(&heimdallv1.Advice{
		HeimdallJobId: "advice-123",
		JobId:         "job-456",
		Issues: []*heimdallv1.Issue{{
			IssueType:  "crash",
			DetectedBy: "",
			Timestamp:  timestamppb.New(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
		}},
	})

	crd, err := advice.ToAdviceCRD()
	if err != nil {
		t.Fatalf("expected advice to convert to CRD, got error: %v", err)
	}
	if len(crd.Spec.Issues) != 1 {
		t.Fatalf("expected 1 issue, got %d", len(crd.Spec.Issues))
	}
	if crd.Spec.Issues[0].DetectedBy != defaultDetectedBy {
		t.Fatalf("expected detectedBy %q, got %q", defaultDetectedBy, crd.Spec.Issues[0].DetectedBy)
	}
}
