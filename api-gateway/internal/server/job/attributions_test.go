// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package job

import (
	"testing"

	v1 "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	jobmanagerBindings "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/jobmanager"
)

// TestAttributionApplicationStatuses protects the public distinction between
// confirmed application, negative outcomes, pending evidence, and absent legacy data.
func TestAttributionApplicationStatuses(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input *jobmanagerBindings.ExclusionApplicationStatus
		want  v1.ExclusionApplicationStatus
	}{
		{"legacy absent", nil, v1.ExclusionApplicationStatus_EXCLUSION_APPLICATION_STATUS_UNSPECIFIED},
		{"pending", statusPointer(jobmanagerBindings.EXCLUSIONAPPLICATIONSTATUS_UNKNOWN), v1.ExclusionApplicationStatus_EXCLUSION_APPLICATION_STATUS_UNKNOWN},
		{"not applicable", statusPointer(jobmanagerBindings.EXCLUSIONAPPLICATIONSTATUS_NOT_APPLICABLE), v1.ExclusionApplicationStatus_EXCLUSION_APPLICATION_STATUS_NOT_APPLICABLE},
		{"not applied", statusPointer(jobmanagerBindings.EXCLUSIONAPPLICATIONSTATUS_NOT_APPLIED), v1.ExclusionApplicationStatus_EXCLUSION_APPLICATION_STATUS_NOT_APPLIED},
		{"applied", statusPointer(jobmanagerBindings.EXCLUSIONAPPLICATIONSTATUS_APPLIED), v1.ExclusionApplicationStatus_EXCLUSION_APPLICATION_STATUS_APPLIED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := transformNodeEventToProto(&jobmanagerBindings.NodeEvent{ExclusionApplied: tc.input})
			if (result.ExclusionApplied == nil) != (tc.input == nil) || result.GetExclusionApplied() != tc.want {
				t.Fatalf("application status = %v, want %v (absent=%v)", result.ExclusionApplied, tc.want, tc.input == nil)
			}
		})
	}
}

func statusPointer(value jobmanagerBindings.ExclusionApplicationStatus) *jobmanagerBindings.ExclusionApplicationStatus {
	return &value
}
