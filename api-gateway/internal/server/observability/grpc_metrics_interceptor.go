// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package observability

import (
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
)

// GrpcMetricsStatsHandler returns a gRPC ServerOption that records OTel metrics
// (request count, duration, message sizes) using the otelgrpc contrib package.
//
// A noop TracerProvider is injected so that only metrics are produced; tracing
// is handled separately by GrpcTracingInterceptor which adds custom attributes
// like request.id.
func GrpcMetricsStatsHandler() grpc.ServerOption {
	return grpc.StatsHandler(otelgrpc.NewServerHandler(
		otelgrpc.WithTracerProvider(noop.NewTracerProvider()),
	))
}
