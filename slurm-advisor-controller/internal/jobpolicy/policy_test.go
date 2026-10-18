// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package jobpolicy_test

import (
	"testing"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/jobpolicy"
)

func TestResolveUsernameSelectionAndDefault(t *testing.T) {
	// The rollout selector has the same meaning under every configured default,
	// including ignore. Removing the selector restores the operator's default.
	for _, defaultMode := range []cluster.JobMode{cluster.JobModeIgnore, cluster.JobModeObserve, cluster.JobModeAct} {
		for _, tt := range []struct {
			name  string
			users []string
			user  string
			want  cluster.JobMode
		}{
			{name: "no selector", user: "alice", want: defaultMode},
			{name: "selected", users: []string{"alice"}, user: "alice", want: cluster.JobModeAct},
			{name: "unselected", users: []string{"alice"}, user: "bob", want: cluster.JobModeObserve},
			{name: "unknown username", users: []string{"alice"}, want: cluster.JobModeObserve},
		} {
			t.Run(string(defaultMode)+"/"+tt.name, func(t *testing.T) {
				got := jobpolicy.Resolve(jobpolicy.Policy{DefaultMode: defaultMode, AllowAct: true, UserAllowlist: tt.users}, cluster.JobExecutionRecord{HeimdallManaged: true, User: tt.user})
				if got.Mode != tt.want || got.RequestedMode != tt.want {
					t.Fatalf("Resolve() = %+v, want requested and effective %s", got, tt.want)
				}
			})
		}
	}
}

func TestResolveExplicitModeAndSafetyCeiling(t *testing.T) {
	for _, tt := range []struct {
		name                                string
		user                                string
		mode                                cluster.JobMode
		specified, valid, managed, allowAct bool
		want, requested                     cluster.JobMode
	}{
		{name: "explicit act outside list", user: "bob", mode: cluster.JobModeAct, specified: true, valid: true, managed: true, allowAct: true, want: cluster.JobModeAct, requested: cluster.JobModeAct},
		{name: "explicit act without username", mode: cluster.JobModeAct, specified: true, valid: true, managed: true, allowAct: true, want: cluster.JobModeAct, requested: cluster.JobModeAct},
		{name: "explicit observe inside list", user: "alice", mode: cluster.JobModeObserve, specified: true, valid: true, managed: true, allowAct: true, want: cluster.JobModeObserve, requested: cluster.JobModeObserve},
		{name: "explicit ignore inside list", user: "alice", mode: cluster.JobModeIgnore, specified: true, valid: true, managed: true, allowAct: true, want: cluster.JobModeIgnore, requested: cluster.JobModeIgnore},
		{name: "invalid explicit mode inside list", user: "alice", mode: "invalid", specified: true, managed: true, allowAct: true, want: cluster.JobModeObserve, requested: jobpolicy.ModeLabelInvalid},
		{name: "selected without acknowledgement", user: "alice", managed: true, want: cluster.JobModeObserve, requested: cluster.JobModeAct},
		{name: "explicit act without acknowledgement", user: "bob", mode: cluster.JobModeAct, specified: true, valid: true, managed: true, want: cluster.JobModeObserve, requested: cluster.JobModeAct},
		{name: "unmanaged selected job cannot act", user: "alice", allowAct: true, want: cluster.JobModeObserve, requested: cluster.JobModeAct},
		{name: "unmanaged explicit request cannot act", user: "bob", mode: cluster.JobModeAct, specified: true, valid: true, allowAct: true, want: cluster.JobModeObserve, requested: cluster.JobModeAct},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := jobpolicy.Resolve(jobpolicy.Policy{DefaultMode: cluster.JobModeAct, AllowAct: tt.allowAct, UserAllowlist: []string{"alice"}}, cluster.JobExecutionRecord{
				HeimdallManaged: tt.managed, User: tt.user,
				ModeRequest: cluster.JobModeRequest{Mode: tt.mode, Specified: tt.specified, Valid: tt.valid},
			})
			if got.Mode != tt.want || got.RequestedMode != tt.requested {
				t.Fatalf("Resolve() = %+v, want effective %s, requested %s", got, tt.want, tt.requested)
			}
		})
	}
}
