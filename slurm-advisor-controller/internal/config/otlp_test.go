// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"bytes"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestFromEnvCollectorConfiguration(t *testing.T) {
	for _, protocol := range []string{"", "grpc", "http", "http/protobuf"} {
		t.Run(protocol, func(t *testing.T) {
			setConfigEnv(t, map[string]string{
				"HEIMDALL_ENDPOINT":           "localhost:8080",
				"OTEL_EXPORTER_OTLP_ENDPOINT": "https://collector.example:4317",
				"OTEL_EXPORTER_OTLP_PROTOCOL": protocol,
				"OTEL_EXPORTER_OTLP_TOKEN":    "test-token",
			})
			cfg, err := FromEnv()
			if err != nil {
				t.Fatal(err)
			}
			if err := cfg.Validate(); err != nil {
				t.Fatal(err)
			}
			wantProtocol := protocol
			if wantProtocol == "" {
				wantProtocol = "grpc"
			}
			if !cfg.OTLP.Enabled() || cfg.OTLP.Protocol != wantProtocol ||
				cfg.OTLP.Endpoint != "https://collector.example:4317" || cfg.OTLP.Token != "test-token" {
				t.Fatal("collector environment was not decoded or defaulted correctly")
			}
		})
	}
}

func TestCollectorRequiresEndpointAndToken(t *testing.T) {
	for _, cfg := range []OTLPConfig{{}, {Endpoint: "https://collector.example"}, {Token: "test-token"}} {
		if cfg.Enabled() {
			t.Fatal("incomplete collector configuration enabled export")
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("console-only configuration rejected: %v", err)
		}
	}
}

func TestCollectorValidationDoesNotExposeSecrets(t *testing.T) {
	const secret = "sensitive-test-credential"
	for _, tc := range []struct {
		name     string
		endpoint string
		protocol string
		token    string
	}{
		{"malformed URL", "https://%" + secret, "grpc", secret},
		{"URL credentials", "https://user:" + secret + "@collector.example", "grpc", secret},
		{"URL query", "https://collector.example?token=" + secret, "grpc", secret},
		{"missing scheme", "collector.example:4317", "grpc", secret},
		{"unsupported scheme", "file:///" + secret, "http", secret},
		{"invalid protocol", "https://collector.example", secret, secret},
		{"header injection", "https://collector.example", "grpc", secret + "\r\nX-Injected: yes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := WithDefaults(Config{
				HeimdallEndpoint: "localhost:8080",
				OTLP:             OTLPConfig{Endpoint: tc.endpoint, Protocol: tc.protocol, Token: tc.token},
			})
			err := cfg.Validate()
			if err == nil {
				t.Fatal("invalid collector configuration accepted")
			}
			var output bytes.Buffer
			logger := zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(&output), zapcore.ErrorLevel))
			logger.Error("validate configuration", zap.Error(err))
			if strings.Contains(output.String(), secret) {
				t.Fatal("configuration error log disclosed a credential")
			}
		})
	}
}
