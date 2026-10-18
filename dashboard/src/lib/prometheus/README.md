# Prometheus Metrics for Dashboard

This module provides Prometheus instrumentation for the NVIDIA Recovery dashboard application.

## Architecture

### Core Components

1. **Registry (`~/server/prometheus.ts`)** - Centralized metrics registry

   - All metrics are pre-configured and exported
   - Default Node.js metrics (memory, CPU, GC) included
   - Custom application metrics for HTTP, API calls, and business logic

2. **Instrumentation (`~/lib/prometheus/instrumentation.ts`)** - Helper utilities

   - Route normalization to prevent high cardinality
   - HTTP request tracking
   - API client wrappers
   - Error tracking

3. **Metrics Endpoint (`~/app/metrics/route.ts`)** - Prometheus scrape target

   - Exposed at `/metrics` (standard Prometheus convention)
   - Unauthenticated (required for Prometheus scraping)
   - Returns metrics in Prometheus text format

4. **Middleware Integration (`~/middleware.ts`)** - Automatic request tracking
   - Tracks all HTTP requests through the dashboard
   - Records duration, status codes, and in-flight requests

## Available Metrics

### HTTP Metrics

- `dashboard_http_request_duration_seconds` - Request duration histogram
- `dashboard_http_requests_total` - Total request counter
- `dashboard_http_requests_in_flight` - Current active requests

### API Metrics (Outbound)

- `dashboard_api_request_duration_seconds` - API call duration
- `dashboard_api_requests_total` - Total API calls
- `dashboard_api_request_errors_total` - Failed API calls

### Application Metrics

- `dashboard_active_sessions` - Current user sessions
- `dashboard_authentication_events_total` - Auth success/failure
- `dashboard_build_info` - Build version and environment

### Default Node.js Metrics

- Memory usage (heap, RSS, external)
- CPU usage
- Event loop lag
- Garbage collection duration
- Process start time

## Usage

### Basic Usage

The metrics endpoint is automatically available at `/metrics` once the application starts.

### Instrumenting API Calls

```typescript
import { instrumentApiClient } from '~/lib/prometheus/instrumentation';

// Create an instrumented fetch client
const jobManager = instrumentApiClient('job-manager');

// Use it like normal fetch
const response = await jobManager('/api/jobs', { method: 'GET' });
```

### Recording Custom Metrics

```typescript
import { apiRequestDuration, activeSessions } from '~/server/prometheus';

// Record a duration
const startTime = Date.now();
// ... do work
const duration = (Date.now() - startTime) / 1000;
apiRequestDuration.observe(
  {
    service: 'job-manager',
    endpoint: '/jobs',
    method: 'GET',
    status_code: 200,
  },
  duration,
);

// Update a gauge
activeSessions.set(42);

// Increment a counter
authenticationEvents.inc({ event_type: 'success', auth_method: 'oauth' });
```
