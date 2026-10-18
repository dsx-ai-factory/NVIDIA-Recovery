// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package observability

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"
)

// GrpcServerStatsHandler instruments gRPC server requests with OTel traces and metrics.
func GrpcServerStatsHandler() grpc.ServerOption {
	return grpc.StatsHandler(otelgrpc.NewServerHandler())
}

// HttpServerMiddleware instruments HTTP server requests with OTel traces and metrics.
func HttpServerMiddleware(serviceName string) func(http.Handler) http.Handler {
	return otelhttp.NewMiddleware(serviceName)
}
