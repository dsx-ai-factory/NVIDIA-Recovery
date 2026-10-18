// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/kelseyhightower/envconfig"
)

type OidcAuthConfig struct {
	ProviderURL   string   `envconfig:"HEIMDALL_OIDC_PROVIDER_URL"`
	ClientID      string   `envconfig:"HEIMDALL_OIDC_CLIENT_ID"`
	ClientSecret  string   `envconfig:"HEIMDALL_OIDC_CLIENT_SECRET"`
	Scopes        []string `envconfig:"HEIMDALL_OIDC_SCOPES" default:"openid,profile,email"`
	CookieKey     []byte   `envconfig:"HEIMDALL_OIDC_COOKIE_KEY" default:""`
	SecureCookie  bool     `envconfig:"HEIMDALL_OIDC_SECURE_COOKIE" default:"true"`
	CookieDomain  string   `envconfig:"HEIMDALL_OIDC_COOKIE_DOMAIN"`
	SubjectClaim  string   `envconfig:"HEIMDALL_OIDC_SUBJECT_CLAIM" default:"sub"`
	ClientIDClaim string   `envconfig:"HEIMDALL_OIDC_CLIENT_ID_CLAIM" default:"client_id"`
	ScopesClaim   string   `envconfig:"HEIMDALL_OIDC_SCOPES_CLAIM" default:"scope"`
}

// JwtAuthConfig holds the public listener's generic JWT provider configuration.
type JwtAuthConfig struct {
	Issuer       string   `envconfig:"HEIMDALL_JWT_ISSUER"`
	Audiences    []string `envconfig:"HEIMDALL_JWT_AUDIENCES"`
	JWKSURL      string   `envconfig:"HEIMDALL_JWT_JWKS_URL"`
	SubjectClaim string   `envconfig:"HEIMDALL_JWT_SUBJECT_CLAIM" default:"sub"`
	// AllowedSubjects optionally limits the mapped subject to exact values.
	AllowedSubjects subjectAllowlist `envconfig:"HEIMDALL_JWT_ALLOWED_SUBJECTS"`
	ClientIDClaim   string           `envconfig:"HEIMDALL_JWT_CLIENT_ID_CLAIM" default:"client_id"`
	ScopesClaim     string           `envconfig:"HEIMDALL_JWT_SCOPES_CLAIM" default:"scope"`
}

type subjectAllowlist []string

// Decode preserves an explicitly empty allowlist while rejecting whitespace
// that envconfig's default slice decoder would turn into unrestricted access.
func (s *subjectAllowlist) Decode(value string) error {
	if value == "" {
		*s = nil
		return nil
	}
	if strings.TrimSpace(value) == "" {
		return errors.New("JWT allowed subjects must not contain only whitespace")
	}
	*s = strings.Split(value, ",")
	return nil
}

type KubernetesAuthConfig struct {
	Enabled                bool   `envconfig:"HEIMDALL_KUBERNETES_AUTH_ENABLED" default:"false"`
	Audience               string `envconfig:"HEIMDALL_KUBERNETES_AUTH_AUDIENCE"`
	ExpectedServiceAccount string `envconfig:"HEIMDALL_KUBERNETES_AUTH_EXPECTED_SERVICE_ACCOUNT"`
}

// AuthConfig holds authentication-related configuration.
type AuthConfig struct {
	Insecure         bool `envconfig:"HEIMDALL_INSECURE" default:"false"`
	OidcEnabled      bool `envconfig:"HEIMDALL_OIDC_ENABLED" default:"true"`
	OidcConfig       OidcAuthConfig
	JwtEnabled       bool `envconfig:"HEIMDALL_JWT_ENABLED" default:"false"`
	JwtConfig        JwtAuthConfig
	KubernetesConfig KubernetesAuthConfig
	LoginRedirectURL string `envconfig:"HEIMDALL_LOGIN_REDIRECT_URL" default:"http://localhost:8080/auth/callback"`
}

// ServerConfig holds server-related configuration.
type ServerConfig struct {
	EnableExternalEndpoints    bool   `envconfig:"HEIMDALL_ENABLE_EXTERNAL_ENDPOINTS" default:"true"`
	GRPCServerEndpoint         string `envconfig:"HEIMDALL_GRPC_SERVER_ENDPOINT" default:"0.0.0.0:9090"`
	HTTPServerEndpoint         string `envconfig:"HEIMDALL_HTTP_SERVER_ENDPOINT" default:"0.0.0.0:8080"`
	InternalGRPCServerEndpoint string `envconfig:"HEIMDALL_INTERNAL_GRPC_SERVER_ENDPOINT" default:"0.0.0.0:9091"`
	InternalHTTPServerEndpoint string `envconfig:"HEIMDALL_INTERNAL_HTTP_SERVER_ENDPOINT" default:"0.0.0.0:8081"`
	URLPrefix                  string `envconfig:"HEIMDALL_URL_PREFIX" default:""`
	InternalURLPrefix          string `envconfig:"HEIMDALL_INTERNAL_URL_PREFIX" default:""`
	CorsAllowOrigin            string `envconfig:"HEIMDALL_CORS_ALLOW_ORIGIN"`
}

// JobConfig holds job-related configuration.
type JobConfig struct {
	Source   string `envconfig:"HEIMDALL_JOB_SOURCE" default:"notimplemented"`
	Endpoint string `envconfig:"HEIMDALL_JOB_MANAGER_ENDPOINT" default:""`
}

// AuditConfig holds downstream Audit Trail service configuration.
type AuditConfig struct {
	// Endpoint is the Audit Trail service base URL used by the gateway.
	Endpoint string `envconfig:"HEIMDALL_AUDIT_ENDPOINT" default:"http://notifications:8080"`
}

// OtelConfig holds OpenTelemetry configuration.
// Standard OTEL_EXPORTER_OTLP_* vars are read from env directly in loadConfig().
type OtelConfig struct {
	Enabled        bool    `envconfig:"HEIMDALL_OTEL_ENABLED" default:"false"`
	ServiceName    string  `envconfig:"HEIMDALL_OTEL_SERVICE_NAME" default:"nvidia-recovery-api-gateway"`
	ServiceVersion string  `envconfig:"HEIMDALL_OTEL_SERVICE_VERSION" default:"1.0.0"`
	Insecure       bool    `envconfig:"HEIMDALL_OTEL_INSECURE" default:"false"`
	SampleRate     float64 `envconfig:"HEIMDALL_OTEL_SAMPLE_RATE" default:"1.0"`
	Protocol       string  // from OTEL_EXPORTER_OTLP_PROTOCOL
	Endpoint       string  // from OTEL_EXPORTER_OTLP_ENDPOINT
	Token          string  // from OTEL_EXPORTER_OTLP_TOKEN
}

// MetricsTelemetryConfig holds Prometheus metrics server config.
type MetricsTelemetryConfig struct {
	Enabled        bool   `envconfig:"HEIMDALL_METRICS_TELEMETRY_ENABLED" default:"true"`
	ServerEndpoint string `envconfig:"HEIMDALL_METRICS_TELEMETRY_SERVER_ENDPOINT" default:"0.0.0.0:9092"`
}

// Config holds the application's configuration.
type Config struct {
	LogFormat        string `envconfig:"HEIMDALL_LOG_FORMAT" default:"text"`
	Auth             AuthConfig
	Server           ServerConfig
	Job              JobConfig
	Audit            AuditConfig
	Otel             OtelConfig
	MetricsTelemetry MetricsTelemetryConfig
}

// loadConfig loads configuration from environment variables.
func loadConfig() (*Config, error) {
	var c Config
	err := envconfig.Process("", &c)
	if err != nil {
		return nil, err
	}
	// Read standard OTel env vars (not HEIMDALL_-prefixed).
	//
	// Auth note: Two env vars handle OTLP authentication:
	//   - OTEL_EXPORTER_OTLP_TOKEN (non-standard, Heimdall-specific): read here
	//     and passed explicitly when creating the OTLP exporter in Go code.
	//   - OTEL_EXPORTER_OTLP_HEADERS (standard OTel spec): set in Helm as
	//     "Authorization=Bearer $(OTEL_EXPORTER_OTLP_TOKEN)" for SDK-level
	//     auto-configuration. Not read by Go code directly.
	// Both are needed because the manual exporter setup in tracing.go uses
	// Token, while any OTel SDK auto-instrumentation reads Headers.
	c.Otel.Protocol = os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")
	c.Otel.Endpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	c.Otel.Token = os.Getenv("OTEL_EXPORTER_OTLP_TOKEN")
	return &c, nil
}
