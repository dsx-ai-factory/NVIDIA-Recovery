// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	t.Setenv("HEIMDALL_API_GATEWAY_URL", "api-gateway.example.test:9090")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.HeimdallAPIProtocol != defaultProtocol {
		t.Fatalf("HeimdallAPIProtocol = %q, want %q", cfg.HeimdallAPIProtocol, defaultProtocol)
	}
	if cfg.PollInterval != defaultPollInterval {
		t.Fatalf("PollInterval = %s, want %s", cfg.PollInterval, defaultPollInterval)
	}
}

func TestLoadRejectsMissingAPIURL(t *testing.T) {
	t.Setenv("HEIMDALL_API_GATEWAY_URL", "")

	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want missing API URL error")
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "unsupported protocol",
			cfg: Config{
				HeimdallAPIURL:      "api-gateway.example.test:9090",
				HeimdallAPIProtocol: "http",
				PollInterval:        time.Second,
			},
		},
		{
			name: "non-positive poll interval",
			cfg: Config{
				HeimdallAPIURL:      "api-gateway.example.test:9090",
				HeimdallAPIProtocol: defaultProtocol,
				PollInterval:        0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cfg.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want validation error")
			}
		})
	}
}
