// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * Prometheus Metrics Registry
 *
 * Centralized registry for all application metrics.
 * This module provides a singleton registry and pre-configured metric collectors
 * for monitoring the dashboard application.
 */

import {
  Counter,
  collectDefaultMetrics,
  Gauge,
  Histogram,
  Registry,
} from "prom-client";

/**
 * Singleton Prometheus registry
 * All metrics should be registered to this registry
 */
export const register = new Registry();

/**
 * Add default Node.js metrics (memory, CPU, event loop, etc.)
 */
collectDefaultMetrics({
  register,
  prefix: "dashboard_",
  gcDurationBuckets: [0.001, 0.01, 0.1, 1, 2, 5],
});

/**
 * HTTP Request Duration Histogram
 * Tracks response time distribution for HTTP requests
 */
export const httpRequestDuration = new Histogram({
  name: "dashboard_http_request_duration_seconds",
  help: "Duration of HTTP requests in seconds",
  labelNames: ["method", "route", "status_code"] as const,
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
  registers: [register],
});

/**
 * HTTP Request Counter
 * Total number of HTTP requests by method, route, and status
 */
export const httpRequestTotal = new Counter({
  name: "dashboard_http_requests_total",
  help: "Total number of HTTP requests",
  labelNames: ["method", "route", "status_code"] as const,
  registers: [register],
});

/**
 * HTTP Request In-Flight Gauge
 * Current number of HTTP requests being processed
 */
export const httpRequestsInFlight = new Gauge({
  name: "dashboard_http_requests_in_flight",
  help: "Current number of HTTP requests being processed",
  labelNames: ["method", "route"] as const,
  registers: [register],
});

/**
 * API Request Duration Histogram
 * Tracks duration of outbound API calls to backend services
 */
export const apiRequestDuration = new Histogram({
  name: "dashboard_api_request_duration_seconds",
  help: "Duration of outbound API requests in seconds",
  labelNames: ["service", "endpoint", "method", "status_code"] as const,
  buckets: [0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30],
  registers: [register],
});

/**
 * API Request Counter
 * Total number of outbound API requests
 */
export const apiRequestTotal = new Counter({
  name: "dashboard_api_requests_total",
  help: "Total number of outbound API requests",
  labelNames: ["service", "endpoint", "method", "status_code"] as const,
  registers: [register],
});

/**
 * API Request Error Counter
 * Total number of failed outbound API requests
 */
export const apiRequestErrors = new Counter({
  name: "dashboard_api_request_errors_total",
  help: "Total number of failed outbound API requests",
  labelNames: ["service", "endpoint", "method", "error_type"] as const,
  registers: [register],
});

/**
 * User Session Gauge
 * Current number of active user sessions
 */
export const activeSessions = new Gauge({
  name: "dashboard_active_sessions",
  help: "Current number of active user sessions",
  registers: [register],
});

/**
 * Authentication Counter
 * Track authentication events (success/failure)
 */
export const authenticationEvents = new Counter({
  name: "dashboard_authentication_events_total",
  help: "Total number of authentication events",
  labelNames: ["event_type", "auth_method"] as const,
  registers: [register],
});

/**
 * Next.js Build Info
 * Static information about the build
 */
export const buildInfo = new Gauge({
  name: "dashboard_build_info",
  help: "Build information",
  labelNames: ["version", "node_env"] as const,
  registers: [register],
});

// Set build info
buildInfo.set(
  {
    version: process.env.npm_package_version || "unknown",
    node_env: process.env.NODE_ENV || "development",
  },
  1,
);

/**
 * Get all metrics in Prometheus format
 */
export async function getMetrics(): Promise<string> {
  return await register.metrics();
}

/**
 * Get content type for Prometheus metrics
 */
export function getMetricsContentType(): string {
  return register.contentType;
}

/**
 * Reset all metrics (useful for testing)
 */
export function resetMetrics(): void {
  register.resetMetrics();
}
