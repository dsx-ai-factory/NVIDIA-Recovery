// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"net/url"
	"strings"

	"github.com/cockroachdb/errors"
)

// OTLPConfig uses the same collector environment contract as the backend services.
type OTLPConfig struct {
	// Endpoint is the collector base URL, including its http or https scheme.
	Endpoint string
	// Protocol selects grpc or HTTP/protobuf (http is an alias).
	Protocol string
	// Token is the bearer credential supplied by OTEL_EXPORTER_OTLP_TOKEN.
	Token string
}

// Enabled requires both the endpoint and token, as in the Python backends.
func (cfg OTLPConfig) Enabled() bool {
	return cfg.Endpoint != "" && cfg.Token != ""
}

// Validate rejects invalid enabled configurations without exposing credentials.
func (cfg OTLPConfig) Validate() error {
	if !cfg.Enabled() {
		return nil
	}
	switch cfg.Protocol {
	case "grpc", "http", "http/protobuf":
	default:
		return errors.New("OTEL_EXPORTER_OTLP_PROTOCOL must be grpc, http, or http/protobuf")
	}
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") ||
		u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("OTEL_EXPORTER_OTLP_ENDPOINT must be an http or https collector URL without credentials, query, or fragment")
	}
	if strings.ContainsAny(cfg.Token, "\r\n") {
		return errors.New("OTEL_EXPORTER_OTLP_TOKEN must not contain newlines")
	}
	return nil
}
