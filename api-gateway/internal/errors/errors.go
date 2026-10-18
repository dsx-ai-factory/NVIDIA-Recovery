// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package errors

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrBadConfig                 = fmt.Errorf("bad config")
	ErrInvalidConfig             = fmt.Errorf("invalid config")
	ErrUninitializedConfig       = fmt.Errorf("unintialized config")
	ErrBadCerts                  = fmt.Errorf("bad certs")
	ErrCertAndKeyRequired        = fmt.Errorf("missing cert or key file when tls is enabled")
	ErrCertOrKeyFileMissing      = fmt.Errorf("either both tls cert path and tls key path should be provided or none")
	ErrCACertRequired            = fmt.Errorf("client-ca-cert-file must be provided")
	ErrBadTokenConfig            = fmt.Errorf("bad token configuration")
	ErrJwtMissing                = fmt.Errorf("missing jwt token")
	ErrJwtMissingKID             = fmt.Errorf("jwt token missing kid header")
	ErrJwtWrongKey               = fmt.Errorf("jwt key not found")
	ErrFetchingToken             = fmt.Errorf("error fetching token")
	ErrPermissionDenied          = fmt.Errorf("permission denied")
	ErrExceedMaxAllowedRetries   = fmt.Errorf("exceed max allowed retries")
	ErrFailedToFetchJWK          = fmt.Errorf("failed to fetch jwk")
	ErrEmptyEventsList           = fmt.Errorf("the events list is empty")
	ErrEventsSizeExceedsLimit    = fmt.Errorf("the total events size exceeds 1MB")
	ErrNegativeValue             = fmt.Errorf("the value must not be negative")
	ErrNonPositiveValue          = fmt.Errorf("the value must  be positive")
	ErrExceedMaxBufferSize       = fmt.Errorf("the size of the adding events exceeds max buffer size")
	ErrExceedMaxEventsListLength = fmt.Errorf("the size of the adding events exceeds max event list length")
	ErrEmptyCollectorID          = fmt.Errorf("invalid metering config, collector id can not be empty")
	ErrEmptyClientSecret         = fmt.Errorf("invalid metering config, client secret can not be empty")
	ErrEmptyClientID             = fmt.Errorf("invalid metering config, client id can not be empty")
	ErrNoCreds                   = fmt.Errorf("invalid metering config, no client credential file nor client id and secrets")
)

type ConfigError struct {
	FieldName string
	Message   string
}

func (c *ConfigError) Error() string {
	fieldInfo := ""
	if c.FieldName != "" {
		fieldInfo = fmt.Sprintf("field: %s ", c.FieldName)
	}
	return fmt.Sprintf("%smessage: %s", fieldInfo, c.Message)
}

type Error interface {
	error

	WithCode(codes.Code) Error
	WithReason(string) Error
	WithOrigin(string) Error
	WithMetadata(md map[string]string) Error

	ID() string
	Origin() string
	Reason() string
	Metadata() map[string]string
	GetMetadata(key string) string
	GRPCStatus() *status.Status
}
