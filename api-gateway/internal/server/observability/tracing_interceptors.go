// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package observability

import (
	"context"
	"net/http"
	"strings"

	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/util"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// GrpcTracingInterceptor creates a gRPC unary server interceptor for tracing.
func GrpcTracingInterceptor(serviceName string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		tracer := otel.Tracer(serviceName)

		// Extract tracing context from gRPC metadata
		md, _ := metadata.FromIncomingContext(ctx)
		carrier := &grpcMetadataCarrier{md: md}
		ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)

		// Start a new span
		ctx, span := tracer.Start(ctx, info.FullMethod,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("rpc.system", "grpc"),
				attribute.String("rpc.service", extractServiceFromMethod(info.FullMethod)),
				attribute.String("rpc.method", extractMethodFromMethod(info.FullMethod)),
			),
		)
		defer span.End()

		// Add request ID to span if available
		if reqID := util.GetRequestIDFromContext(ctx); reqID != "" {
			span.SetAttributes(attribute.String("request.id", reqID))
		}

		// Call the handler
		resp, err := handler(ctx, req)

		// Set span status based on error
		if err != nil {
			span.RecordError(err)
			if st, ok := status.FromError(err); ok {
				span.SetStatus(codes.Error, st.Message())
				span.SetAttributes(
					attribute.String("rpc.grpc.status_code", st.Code().String()),
					attribute.Int("rpc.grpc.status_code_int", int(st.Code())),
				)
			} else {
				span.SetStatus(codes.Error, err.Error())
			}
		} else {
			span.SetStatus(codes.Ok, "")
			span.SetAttributes(attribute.String("rpc.grpc.status_code", "OK"))
		}

		return resp, err
	}
}

// HttpTracingMiddleware creates an HTTP middleware for tracing.
func HttpTracingMiddleware(serviceName string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tracer := otel.Tracer(serviceName)
			scheme := requestScheme(r)

			// Extract tracing context from HTTP headers
			ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
			r = r.WithContext(ctx)

			// Start a new span
			ctx, span := tracer.Start(ctx, r.Method+" "+routePrefix(r.URL.Path),
				trace.WithSpanKind(trace.SpanKindServer),
				trace.WithAttributes(
					attribute.String("http.request.method", r.Method),
					attribute.String("url.scheme", scheme),
					attribute.String("server.address", r.Host),
					attribute.String("url.path", r.URL.Path),
					attribute.String("user_agent.original", r.UserAgent()),
				),
			)
			defer span.End()

			// Add request ID to span if available
			if reqID := util.GetRequestIDFromContext(ctx); reqID != "" {
				span.SetAttributes(attribute.String("request.id", reqID))
			}

			// Wrap response writer to capture status code
			wrapped := newHTTPMetricsResponseWriter(w)

			// Inject tracing context into response headers
			otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(w.Header()))

			// Update request context
			r = r.WithContext(ctx)

			// Call the next handler
			next.ServeHTTP(wrapped, r)

			// Set span attributes based on response
			span.SetAttributes(
				attribute.Int("http.response.status_code", wrapped.statusCode),
			)

			// Set span status based on HTTP status code
			if wrapped.statusCode >= 400 {
				span.SetStatus(codes.Error, http.StatusText(wrapped.statusCode))
			} else {
				span.SetStatus(codes.Ok, "")
			}
		})
	}
}

func requestScheme(r *http.Request) string {
	if forwardedProto := r.Header.Get("X-Forwarded-Proto"); forwardedProto != "" {
		proto, _, _ := strings.Cut(forwardedProto, ",")
		if scheme := strings.ToLower(strings.TrimSpace(proto)); scheme != "" {
			return scheme
		}
	}

	if r.TLS != nil {
		return "https"
	}

	return "http"
}

// grpcMetadataCarrier implements TextMapCarrier for gRPC metadata.
type grpcMetadataCarrier struct {
	md metadata.MD
}

func (c *grpcMetadataCarrier) Get(key string) string {
	values := c.md.Get(key)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (c *grpcMetadataCarrier) Set(key, value string) {
	c.md.Set(key, value)
}

func (c *grpcMetadataCarrier) Keys() []string {
	keys := make([]string, 0, len(c.md))
	for k := range c.md {
		keys = append(keys, k)
	}
	return keys
}

// extractServiceFromMethod extracts the service name from the full gRPC method.
//
// Given a fullMethod of "/package.service/method"
// Returns "service"
func extractServiceFromMethod(fullMethod string) string {
	fullMethod = strings.TrimPrefix(fullMethod, "/")
	servicePart, _, ok := strings.Cut(fullMethod, "/")
	if !ok {
		return ""
	}
	// servicePart is "package.service"; we want the part after the last dot.
	dotIdx := strings.LastIndex(servicePart, ".")
	if dotIdx == -1 {
		return servicePart
	}
	return servicePart[dotIdx+1:]
}

// extractMethodFromMethod extracts the method name from the full gRPC method.
//
// Given a fullMethod of "/package.service/method"
// Returns "method"
func extractMethodFromMethod(fullMethod string) string {
	fullMethod = strings.TrimPrefix(fullMethod, "/")
	_, method, ok := strings.Cut(fullMethod, "/")
	if !ok || method == "" {
		return ""
	}
	return method
}
