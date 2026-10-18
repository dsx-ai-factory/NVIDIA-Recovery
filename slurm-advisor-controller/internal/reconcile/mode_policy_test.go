// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"testing"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

func TestResolveEffectiveMode(t *testing.T) {
	tests := []struct {
		name   string
		policy ModePolicy
		job    cluster.JobExecutionRecord
		want   EffectiveModeDecision
	}{
		{
			name:   "ignore default without comment",
			policy: ModePolicy{DefaultMode: cluster.JobModeIgnore},
			want:   EffectiveModeDecision{Mode: cluster.JobModeIgnore, RequestedMode: cluster.JobModeIgnore, Reason: ModeReasonDefault},
		},
		{
			name:   "observe default without comment",
			policy: ModePolicy{DefaultMode: cluster.JobModeObserve},
			want:   EffectiveModeDecision{Mode: cluster.JobModeObserve, RequestedMode: cluster.JobModeObserve, Reason: ModeReasonDefault},
		},
		{
			name:   "act default without comment cannot authorize action",
			policy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
			want:   EffectiveModeDecision{Mode: cluster.JobModeObserve, RequestedMode: cluster.JobModeAct, Reason: ModeReasonMissingComment},
		},
		{
			name:   "ignore default with valid comment and no mode",
			policy: ModePolicy{DefaultMode: cluster.JobModeIgnore},
			job:    validCommentWithoutMode(),
			want:   EffectiveModeDecision{Mode: cluster.JobModeIgnore, RequestedMode: cluster.JobModeIgnore, Reason: ModeReasonDefault},
		},
		{
			name:   "observe default with valid comment and no mode",
			policy: ModePolicy{DefaultMode: cluster.JobModeObserve},
			job:    validCommentWithoutMode(),
			want:   EffectiveModeDecision{Mode: cluster.JobModeObserve, RequestedMode: cluster.JobModeObserve, Reason: ModeReasonDefault},
		},
		{
			name:   "act default with valid comment and no mode",
			policy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
			job:    validCommentWithoutMode(),
			want:   EffectiveModeDecision{Mode: cluster.JobModeAct, RequestedMode: cluster.JobModeAct, Reason: ModeReasonDefault},
		},
		{
			name:   "job ignore overrides act default",
			policy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
			job:    jobWithMode(cluster.JobModeIgnore),
			want:   EffectiveModeDecision{Mode: cluster.JobModeIgnore, RequestedMode: cluster.JobModeIgnore, Reason: ModeReasonJobOverride},
		},
		{
			name:   "job observe overrides act default",
			policy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
			job:    jobWithMode(cluster.JobModeObserve),
			want:   EffectiveModeDecision{Mode: cluster.JobModeObserve, RequestedMode: cluster.JobModeObserve, Reason: ModeReasonJobOverride},
		},
		{
			name:   "job act overrides observe default when allowed",
			policy: ModePolicy{DefaultMode: cluster.JobModeObserve, AllowAct: true},
			job:    jobWithMode(cluster.JobModeAct),
			want:   EffectiveModeDecision{Mode: cluster.JobModeAct, RequestedMode: cluster.JobModeAct, Reason: ModeReasonJobOverride},
		},
		{
			name:   "job act is capped when denied",
			policy: ModePolicy{DefaultMode: cluster.JobModeObserve},
			job:    jobWithMode(cluster.JobModeAct),
			want:   EffectiveModeDecision{Mode: cluster.JobModeObserve, RequestedMode: cluster.JobModeAct, Reason: ModeReasonActionDenied},
		},
		{
			name:   "invalid job mode is observe",
			policy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
			job: cluster.JobExecutionRecord{
				HeimdallManaged: true,
				ModeRequest: cluster.JobModeRequest{
					Mode:      cluster.JobMode(" act "),
					Specified: true,
				},
			},
			want: EffectiveModeDecision{Mode: cluster.JobModeObserve, RequestedMode: ModeLabelInvalid, Reason: ModeReasonInvalidRequest},
		},
		{
			name:   "invalid default fails closed",
			policy: ModePolicy{DefaultMode: cluster.JobMode("invalid"), AllowAct: true},
			job:    validCommentWithoutMode(),
			want:   EffectiveModeDecision{Mode: cluster.JobModeObserve, RequestedMode: ModeLabelInvalid, Reason: ModeReasonInvalidDefault},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ResolveEffectiveMode(tt.policy, tt.job); got != tt.want {
				t.Fatalf("ResolveEffectiveMode() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func validCommentWithoutMode() cluster.JobExecutionRecord {
	return cluster.JobExecutionRecord{
		HeimdallManaged: true,
		ModeRequest:     cluster.JobModeRequest{Valid: true},
	}
}

func jobWithMode(mode cluster.JobMode) cluster.JobExecutionRecord {
	return cluster.JobExecutionRecord{
		HeimdallManaged: true,
		ModeRequest: cluster.JobModeRequest{
			Mode:      mode,
			Specified: true,
			Valid:     true,
		},
	}
}
