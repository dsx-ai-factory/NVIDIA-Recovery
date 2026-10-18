// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"context"

	"go.uber.org/zap"
)

type ContextKey string

const (
	RequestIDKey    ContextKey = "requestID"
	RequestIDHeader string     = "X-Request-ID"
	ZapLoggerKey    ContextKey = "zapLogger"
)

// getRequestIDFromContext extracts the request ID from context
func GetRequestIDFromContext(ctx context.Context) string {
	if reqID, ok := ctx.Value(RequestIDKey).(string); ok {
		return reqID
	}
	return ""
}

func GetZapLoggerFromContext(ctx context.Context) *zap.Logger {
	if zapLogger, ok := ctx.Value(ZapLoggerKey).(*zap.Logger); ok {
		return zapLogger
	}

	return zap.L()
}

func SetZapLoggerInContext(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, ZapLoggerKey, logger)
}

func GetZapLoggerFromContextWith(ctx context.Context, fields ...zap.Field) (context.Context, *zap.Logger) {
	logger := GetZapLoggerFromContext(ctx).With(fields...)
	ctx = SetZapLoggerInContext(ctx, logger)
	return ctx, logger
}
