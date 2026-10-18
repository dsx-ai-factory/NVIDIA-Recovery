// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package tlsconfig defines gateway TLS settings without transport dependencies.
package tlsconfig

import "github.com/cockroachdb/errors"

// Config selects verified TLS or the legacy plaintext internal transport.
type Config struct {
	// Enabled enables TLS with certificate and hostname verification.
	Enabled bool
	// CAFile replaces system roots with this PEM-encoded CA bundle when nonempty.
	CAFile string
	// ServerName overrides the certificate name; empty uses the endpoint hostname.
	ServerName string
}

// Validate rejects TLS settings that would otherwise be silently ignored.
func (cfg Config) Validate() error {
	if !cfg.Enabled && (cfg.CAFile != "" || cfg.ServerName != "") {
		return errors.New("gateway TLS CA file and server name require TLS enabled")
	}
	return nil
}
