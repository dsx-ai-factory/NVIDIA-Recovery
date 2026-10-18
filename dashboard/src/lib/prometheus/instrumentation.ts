// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * Prometheus Instrumentation Utilities
 *
 * Helper functions for instrumenting HTTP requests, API calls, and other operations
 */

import {
  apiRequestDuration,
  apiRequestErrors,
  apiRequestTotal,
  httpRequestDuration,
  httpRequestsInFlight,
  httpRequestTotal,
} from "~/server/prometheus";

/**
 * Normalize route paths to avoid high cardinality
 * Replaces dynamic segments with placeholders
 *
 * @example
 * normalizeRoute('/api/jobs/12345') => '/api/jobs/:id'
 * normalizeRoute('/dashboard/workflow/job/abc-def') => '/dashboard/workflow/job/:id'
 */
function normalizeRoute(pathname: string): string {
  return (
    pathname
      // Replace UUIDs and long IDs
      .replace(
        /\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi,
        "/:uuid",
      )
      .replace(/\/[0-9a-f]{32,}/gi, "/:hash")
      // Replace numeric IDs
      .replace(/\/\d+/g, "/:id")
      // Replace session IDs (format: xxx-xxx-xxx)
      .replace(/\/[a-z0-9]+-[a-z0-9]+-[a-z0-9]+/gi, "/:sessionId")
      // Limit to first 5 segments to prevent explosion
      .split("/")
      .slice(0, 6)
      .join("/")
  );
}

/**
 * Record HTTP request metrics
 *
 * @param request - NextRequest object
 * @param statusCode - HTTP status code
 * @param durationSeconds - Request duration in seconds
 */
function recordHttpRequest(
  method: string,
  route: string,
  statusCode: number,
  durationSeconds: number,
): void {
  httpRequestDuration.observe(
    { method, route, status_code: statusCode },
    durationSeconds,
  );
  httpRequestTotal.inc({ method, route, status_code: statusCode });
}

/**
 * Increments the in-flight HTTP request gauge for a specific method and normalized route.
 * Use at the start of each HTTP request to signal that a request is being processed.
 *
 * This function does not measure duration or record counts itself; it is used solely
 * for in-flight request tracking. Make sure to call {@link endHttpRequest} when the
 * request is finished to decrement the gauge and optionally record metrics.
 *
 * @param method - HTTP method, e.g. "GET", "POST"
 * @param route - The request route or path (will be normalized to reduce cardinality)
 *
 * @example
 * startHttpRequest('GET', '/api/jobs/123');   // Increments /api/jobs/:id in-flight
 */
export function startHttpRequest(method: string, route: string): void {
  const normalizedRoute = normalizeRoute(route);
  // Increment in-flight gauge
  httpRequestsInFlight.inc({ method, route: normalizedRoute });
}

/**
 * Decrements the in-flight HTTP request gauge for a specific method and normalized route,
 * and records metrics on the request duration and status code.
 *
 * This should be called when an HTTP request finishes processing. The function will
 * decrement the in-flight gauge and record corresponding Prometheus metrics for
 * request count and duration.
 *
 * @param method - HTTP method, e.g. "GET", "POST"
 * @param route - The request route or path (will be normalized to reduce cardinality)
 * @param statusCode - HTTP status code of the response
 * @param durationMs - Duration of the request in milliseconds
 *
 * @example
 * endHttpRequest('POST', '/api/jobs/123', 200, 47); // Observes duration/count for /api/jobs/:id
 */
export function endHttpRequest(
  method: string,
  route: string,
  statusCode: number,
  durationMs: number,
): void {
  const durationSeconds = durationMs / 1000;
  const normalizedRoute = normalizeRoute(route);
  // Decrement in-flight gauge
  httpRequestsInFlight.dec({ method, route: normalizedRoute });

  // Record metrics
  recordHttpRequest(method, normalizedRoute, statusCode, durationSeconds);
}

/**
 * Record outbound API request metrics
 *
 * @param service - Backend service name (e.g., 'job-manager', 'notifications')
 * @param endpoint - API endpoint path
 * @param method - HTTP method
 * @param statusCode - HTTP status code
 * @param durationSeconds - Request duration in seconds
 */
export function recordApiRequest(
  service: string,
  endpoint: string,
  method: string,
  statusCode: number,
  durationSeconds: number,
): void {
  const normalizedEndpoint = normalizeRoute(endpoint);

  apiRequestDuration.observe(
    { service, endpoint: normalizedEndpoint, method, status_code: statusCode },
    durationSeconds,
  );
  apiRequestTotal.inc({
    service,
    endpoint: normalizedEndpoint,
    method,
    status_code: statusCode,
  });
}

/**
 * Record API request error
 *
 * @param service - Backend service name
 * @param endpoint - API endpoint path
 * @param method - HTTP method
 * @param errorType - Type of error (e.g., 'timeout', 'network', 'server_error')
 */
export function recordApiError(
  service: string,
  endpoint: string,
  method: string,
  errorType: string,
): void {
  const normalizedEndpoint = normalizeRoute(endpoint);
  apiRequestErrors.inc({
    service,
    endpoint: normalizedEndpoint,
    method,
    error_type: errorType,
  });
}

/**
 * Create an instrumented API client wrapper
 *
 * @example
 * const instrumentedFetch = instrumentApiClient('job-manager');
 * const response = await instrumentedFetch('/api/jobs', { method: 'GET' });
 */
export function instrumentApiClient(serviceName: string) {
  return async (url: string, init?: RequestInit): Promise<Response> => {
    const method = init?.method ?? "GET";
    const startTime = Date.now();

    try {
      const response = await fetch(url, init);
      const duration = (Date.now() - startTime) / 1000;

      recordApiRequest(serviceName, url, method, response.status, duration);

      return response;
    } catch (error) {
      const errorType = error instanceof TypeError ? "network" : "unknown";
      recordApiError(serviceName, url, method, errorType);
      throw error;
    }
  };
}

/**
 * Utility to safely extract error type from unknown error
 */
export function getErrorType(error: unknown): string {
  if (error instanceof TypeError) return "network";
  if (error instanceof Error && error.message.includes("timeout"))
    return "timeout";
  if (error instanceof Error && error.message.includes("abort"))
    return "aborted";
  return "unknown";
}
