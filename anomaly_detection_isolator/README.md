# Anomaly Detection Isolator (ADI)

The Anomaly Detection Isolator (ADI) is an NVIDIA Recovery component that analyzes
diagnostic data to isolate unhealthy nodes during large-scale AI training
jobs, enabling node exclusion and automated restart on healthy infrastructure.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Development](#development)
- [Testing](#testing)
- [API Documentation](#api-documentation)
- [Contributing](#contributing)

## Overview

ADI is designed to:

1. **Detect and Analyze Anomalies**: Consume job anomaly events from the event bus
2. **Isolate Unhealthy Nodes**: Analyze system logs, application logs, and metrics to identify problematic nodes
3. **Attribution**: Determine root causes of failures (hardware, software, environment, network)
4. **Publish Results**: Send node isolation results back to the event bus for remediation

Attribution engine:
- **FACT**: Failure Analysis and classification tool - the attribution engine used by ADI

## Architecture

### Component Diagram

```
+-----------------+
|   Job Manager   |
+--------+--------+
         | Job details, including status and anomalies to process.
         v
+--------------------------------------------------+
|       Anomaly Detection Isolator (ADI)           |
|                                                  |
|  +--------------+    +-----------------------+   |
|  |   Service    |--->| Anomaly Processor     |   |
|  |   (Main)     |    +-----------+-----------+   |
|  +------+-------+                |               |
|         |                        v               |
|         |            +-----------------------+   |
|         |            |  Attribution Engine   |   |
|         |            |  - FACT               |   |
|         |            +-----------------------+   |
|         |                                        |
|         v                                        |
|  +--------------+                                |
|  | Result       |                                |
|  | Publisher    |                                |
|  +------+-------+                                |
+---------+----------------------------------------+
          |
          v
+-------------------+
|     Event Bus     |
|     (RabbitMQ)    |
+-------------------+
```

### Data Flow

1. **Event Reception**: ADI subscribes to job anomaly events from RabbitMQ
2. **Job Status Retrieval**: Fetches job details from Job Manager
3. **Diagnostics Collection**:
   - System logs from Loki (syslogs, kernel logs)
   - Application logs from Loki (job output, framework logs)
   - Metrics from Prometheus (network, GPU, node health)
4. **Attribution Analysis**:
   - **Fast Signals**: Initial quick analysis using available data
   - **Slow Signals**: Delayed in-depth analysis after job completion
5. **Node Isolation**: Identifies nodes causing anomalies
6. **Result Publishing**: Sends attribution results to event bus with optional custom output queue

## Features

### Core Capabilities

- **FACT-based Attribution**: Uses the FACT engine for log/metric attribution
- **Fast and Slow Signal Processing**: Immediate and delayed analysis modes
- **Custom Output Routing**: Per-event custom output queue support for flexible message routing
- **Comprehensive Telemetry**: Integration with Loki for logs and Prometheus for metrics
- **Node Exclusion Logic**: Smart filtering and sanitization of attribution results
- **Distributed Tracing**: OpenTelemetry integration for observability

### Attribution Types

- **HARDWARE**: GPU failures, memory errors, cooling issues
- **SOFTWARE**: Framework bugs, driver issues, library conflicts
- **ENVIRONMENT**: Configuration problems, resource exhaustion
- **NETWORK**: Communication failures, InfiniBand/Ethernet issues
- **NONE**: No clear attribution found

### Exclusion Types

- **CONFIRMED**: High confidence node should be excluded
- **SUSPECTED**: Medium confidence, potential issue
- **NONE**: No exclusion recommended

## Installation

### Prerequisites

- Python 3.12+
- `uv` for dependency management
- Access to operator-managed dependencies:
  - RabbitMQ event bus
  - Loki log aggregation
  - Prometheus metrics
  - Job Manager API
  - FACT service (optional)

### Setup

1. **Clone the repository**:
   ```bash
   cd anomaly_detection_isolator
   ```

2. **Install dependencies**:
   ```bash
   uv sync
   ```

3. **Set up environment**:
   ```bash
   # Create .env file with required configuration
   cp .env.example .env
   # Edit .env with your environment-specific values
   ```

## Configuration

ADI is configured via environment variables. Key settings include:

### RabbitMQ Configuration
```bash
RABBITMQ_URL=rabbitmq.example.com
RABBITMQ_PORT=5672
RABBITMQ_VHOST=/heimdall
RABBITMQ_USER=adi_user
RABBITMQ_PASSWORD=<password>
ATTRIBUTION_REQUEST_QUEUE=job_updates_attribution_request
ISOLATOR_COMPLETED_EXCHANGE=aie_eventbus_exchange
```

The result exchange must match Job Manager’s `anomaly_isolator_exchange`.
The Helm defaults and standalone JM configuration use the value above.

### Attribution Engine Configuration
```bash
ATTRIBUTION_ENGINES=FACT  # Only FACT is supported
FACT_URL=https://fact.example.com
```

### Telemetry Configuration
```bash
LOKI_URL=https://loki.example.com
PROMETHEUS_URL=https://prometheus.example.com
PROMETHEUS_TOKEN_PATH=/path/to/token
```

### Service Configuration
```bash
NUM_WORKER_THREADS=10
SLOW_SIGNALS_ENABLED=true
SLOW_SIGNALS_DELAY_SECONDS=300
MAX_ISOLATED_NODES=3
```

See `anomaly_detection_isolator/settings.py` for complete configuration options.

## Usage

### Running the Service

```bash
uv run --locked anomaly-detection-isolator
```

### Event Message Format

ADI consumes events with the following structure:

```json
{
  "job_id": "123456",
  "job_attempt_index": 0,
  "anomaly_time": "2024-03-24T10:30:00Z",
  "anomaly_type": "HANG",
  "allocated_nodes": ["node1", "node2", "node3"],
  "start_time": "2024-03-24T09:00:00Z",
  "output_queue": "custom.queue.name"  // Optional: custom output queue for this event
}
```

### Output Message Format

ADI publishes attribution results:

```json
{
  "job_id": "123456",
  "job_attempt_index": 0,
  "anomaly_type": "HANG",
  "error_attributions": {
    "{\"code\": 0, \"message\": \"HANG\", \"description\": \"Node health check failure\"}": [
      {
        "node": "node2",
        "timestamp": "2024-03-24T10:30:00Z",
        "job_id": "123456",
        "error_attribution": 2,  // HARDWARE
        "error_attribution_type": 2,  // FAST
        "error_attribution_origin": 3,  // ADI
        "error_attribution_sources": [3, 4],  // SYSLOGS, NODE_METRICS
        "exclusion_type": 3,  // CONFIRMED
        "fact_status": 1  // NONE
      }
    ]
  }
}
```

## Development

### Project Structure

```
anomaly_detection_isolator/
├── anomaly_detection_isolator/
│   ├── main.py                 # Entry point
│   ├── service.py              # Main service logic, event handling
│   ├── anomaly_processor.py    # Attribution processing
│   ├── settings.py             # Configuration management
│   ├── loki.py                 # Loki client for log fetching
│   ├── metrics_fetcher.py      # Prometheus metrics client
│   ├── job_manager_client.py   # Job Manager API client
│   └── fact_client.py          # FACT attribution client
├── tests/
│   ├── test_service.py
│   ├── test_anomaly_processor.py
│   ├── test_loki.py
│   └── ...
├── pyproject.toml              # uv dependencies and scripts
└── README.md
```

### Key Components

#### `service.py`
Main service orchestration:
- RabbitMQ event consumption
- Worker thread management
- Event routing (fast/slow signals)
- Result publishing with custom queue support

#### `anomaly_processor.py`
Attribution logic:
- Telemetry collection (logs, metrics)
- Attribution engine coordination
- Result sanitization and validation

#### `loki.py`
Log fetching:
- System logs (syslogs, kernel logs)
- Application logs (job output)
- LogQL query construction
- Support for standard and Lepton deployments

## Testing

### Running Tests

```bash
# Run all tests
uv run --locked pytest

# Run with coverage
uv run --locked pytest --cov

# Run specific test file
uv run --locked pytest tests/test_service.py

# Run specific test
uv run --locked pytest tests/test_service.py::TestAnomalyDetectionIsolationServiceTests::test_process_attribution_request_event_basic -v
```

### Test Coverage

The project maintains comprehensive test coverage:
- Unit tests for all major components
- Integration tests for event processing
- Mock-based testing for external dependencies
- 110+ passing tests

### Testing Requirements

Per the repository agent guide:
- **All new features MUST include tests**
- **Tests must pass before code changes are considered complete**
- Mock external dependencies (RabbitMQ, etcd, Loki, Prometheus)
- Verify both success and error cases

## API Documentation

### Main Entry Points

#### `AnomalyDetectionIsolator.setup_event_bus_and_start_consuming()`
Starts the service and begins consuming events from RabbitMQ.

#### `_process_attribution_request_event()`
Handles job anomaly events with fast signal processing.

#### `_process_slow_signals_event()`
Handles delayed attribution requests for in-depth analysis.

#### `_publish_result_message_to_event_bus(message, delivery_tag, output_queue=None)`
Publishes attribution results to `isolator_completed_exchange` with routing key
`anomaly.isolation.bad_nodes`; Job Manager owns the normal result queue and
binding. ADI does not declare a default output queue. If `output_queue` is
provided, ADI declares and binds that custom queue using its name as the routing
key. See the [queue retirement notes](../docs/architecture/slurm-advisor-controller.md#retired-arm-attribution-result-queue)
for startup ordering and existing broker cleanup.

### Key Classes

#### `AnomalyProcessor`
Main attribution processing class with methods:
- `process_anomaly_with_fast_signals()`: Quick attribution analysis
- `process_anomaly_with_slow_signals()`: Delayed in-depth analysis
- `_sanitize_attribution_result()`: Filter and validate results

#### `JobManagerClient`
Interface to Job Manager API:
- `get_job_status()`: Retrieve job execution details

#### `FACTClient`
FACT attribution engine client:
- `create_attribution_request()`: Submit attribution request
- `get_attribution()`: Retrieve attribution results
- `supply_logs()`: Send logs/metrics to FACT

## Contributing

### Development Workflow

1. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make changes and add tests**:
   - All code changes must include corresponding tests
   - Follow existing code style and patterns
   - Update documentation as needed

3. **Run tests locally**:
   ```bash
   uv run --locked pytest
   ```

4. **Pre-commit checks**:
   - Black (Python formatting, line length 120)
   - isort (import sorting)
   - Standard linting checks

5. **Create merge request**:
   - Ensure CI/CD pipeline passes
   - Request code review
   - Address feedback

### Code Style

- Follow PEP 8 with line length limit of 120
- Use type hints for function signatures
- Write descriptive docstrings for public methods
- Keep functions focused and testable

### Logging

Use structured logging with appropriate levels:
```python
logger.info(f"Processing job {job_id} attempt {job_attempt_index}")
logger.warning(f"Failed to fetch logs from Loki: {error}")
logger.error(f"Attribution processing failed for job {job_id}", exc_info=True)
```

## Troubleshooting

### Common Issues

**RabbitMQ Connection Failures**
- Check network connectivity to RabbitMQ
- Verify credentials and vhost configuration
- Ensure queue and exchange are properly declared

**Loki Query Failures**
- Verify Loki URL and authentication token
- Check LogQL query syntax
- Ensure proper time range for queries

**FACT Attribution Errors**
- Verify FACT service availability
- Check FACT URL configuration
- Review FACT error lists configuration

**High Memory Usage**
- Reduce `NUM_WORKER_THREADS` if needed
- Check for log fetching size limits
- Monitor Prometheus metrics

## Metrics and Observability

ADI exports Prometheus metrics:
- `adi_fast_signals_anomaly_processor_errors`: Fast signal processing errors
- `adi_slow_signals_anomaly_processor_errors`: Slow signal processing errors
- `adi_eventbus_publish_errors`: Event publishing failures

OpenTelemetry spans:
- `job_anomaly.attribution`: Attribution processing traces
- Includes job_id, anomaly_type, attribution_type attributes

## License

Licensed under Apache-2.0. See `LICENSE.txt` in the repository root.

## Support

For issues and questions, use the public repository issue tracker.
- Check `AGENTS.md` for development guidelines
