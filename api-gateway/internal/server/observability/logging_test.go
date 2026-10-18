// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package observability

import (
	"context"
	"testing"

	sdklog "go.opentelemetry.io/otel/sdk/log"
)

func TestLoggingProvider_ShutdownNil(t *testing.T) {
	var lp *LoggingProvider
	if err := lp.Shutdown(context.Background()); err != nil {
		t.Fatalf("unexpected error shutting down nil provider: %v", err)
	}
}

func TestLoggingProvider_ProviderNil(t *testing.T) {
	var lp *LoggingProvider
	if lp.Provider() != nil {
		t.Error("expected nil provider")
	}
}

func TestLoggingProvider_Provider(t *testing.T) {
	inner := sdklog.NewLoggerProvider()
	defer inner.Shutdown(context.Background()) //nolint:errcheck

	lp := &LoggingProvider{provider: inner}
	if lp.Provider() != inner {
		t.Error("expected provider to match")
	}
}
