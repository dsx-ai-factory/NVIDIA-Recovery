// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"slices"
	"testing"
)

func TestLoadConfigAllowedSubjects(t *testing.T) {
	const subject = "system:serviceaccount:slinky:sac"
	const prefixedKey = "AUTH_JWTCONFIG_HEIMDALL_JWT_ALLOWED_SUBJECTS"
	for _, tt := range []struct {
		name     string
		value    string
		unset    bool
		prefixed bool
		want     []string
		wantErr  bool
	}{
		{name: "unset remains unrestricted", unset: true},
		{name: "empty remains unrestricted"},
		{name: "spaces rejected", value: "   ", wantErr: true},
		{name: "mixed whitespace rejected", value: " \t\n", wantErr: true},
		{name: "single subject", value: subject, want: []string{subject}},
		{name: "multiple subjects", value: subject + ",other-client", want: []string{subject, "other-client"}},
		{name: "prefixed whitespace overrides valid unprefixed value", value: " \t", prefixed: true, wantErr: true},
		{name: "prefixed empty remains unrestricted", prefixed: true},
		{name: "prefixed subject", value: "other-client", prefixed: true, want: []string{"other-client"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HEIMDALL_JWT_ALLOWED_SUBJECTS", tt.value)
			t.Setenv(prefixedKey, "")
			if err := os.Unsetenv(prefixedKey); err != nil {
				t.Fatal(err)
			}
			if tt.prefixed {
				t.Setenv("HEIMDALL_JWT_ALLOWED_SUBJECTS", subject)
				t.Setenv(prefixedKey, tt.value)
			}
			if tt.unset {
				if err := os.Unsetenv("HEIMDALL_JWT_ALLOWED_SUBJECTS"); err != nil {
					t.Fatal(err)
				}
			}
			cfg, err := loadConfig()
			if tt.wantErr {
				if err == nil || cfg != nil {
					t.Fatal("malformed subject restriction must fail configuration loading")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(cfg.Auth.JwtConfig.AllowedSubjects, tt.want) {
				t.Fatalf("AllowedSubjects = %v, want %v", cfg.Auth.JwtConfig.AllowedSubjects, tt.want)
			}
		})
	}
}
