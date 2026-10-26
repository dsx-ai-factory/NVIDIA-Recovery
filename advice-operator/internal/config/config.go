// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package config decodes and validates Advice operator configuration.
package config

import (
	"time"

	"github.com/cockroachdb/errors"
	"github.com/kelseyhightower/envconfig"
)

const (
	defaultProtocol     = "grpc"
	defaultPollInterval = 30 * time.Second
)

// Config holds the operator configuration.
type Config struct {
	// HeimdallAPIURL is the address of the Heimdall gRPC server.
	HeimdallAPIURL string `envconfig:"HEIMDALL_API_GATEWAY_URL"`

	// HeimdallAPIProtocol is the protocol used for the Heimdall API.
	HeimdallAPIProtocol string `envconfig:"HEIMDALL_API_PROTOCOL"`

	// HeimdallAPIInsecureSkipVerify disables TLS certificate verification.
	HeimdallAPIInsecureSkipVerify bool `envconfig:"HEIMDALL_API_INSECURE_SKIP_VERIFY"`

	// PollInterval is how often the operator polls for advice updates.
	PollInterval time.Duration `envconfig:"POLL_INTERVAL"`
}

// Load decodes configuration from the process environment and validates it.
func Load() (Config, error) {
	cfg := Config{
		HeimdallAPIProtocol: defaultProtocol,
		PollInterval:        defaultPollInterval,
	}
	if err := envconfig.Process("", &cfg); err != nil {
		return Config{}, errors.Wrap(err, "decode Advice operator configuration")
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks the semantic constraints required to start the operator.
func (c Config) Validate() error {
	if c.HeimdallAPIURL == "" {
		return errors.New("HEIMDALL_API_GATEWAY_URL must not be empty")
	}
	if c.HeimdallAPIProtocol != defaultProtocol {
		return errors.Newf("HEIMDALL_API_PROTOCOL must be %q", defaultProtocol)
	}
	if c.PollInterval <= 0 {
		return errors.New("POLL_INTERVAL must be greater than zero")
	}
	return nil
}
