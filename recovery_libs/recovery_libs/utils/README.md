# Utils Module

NVIDIA Recovery utilities module.

## Logs

- Structured JSON format for OTLP delivery
- Automatic OTLP integration when configured
- Support for both HTTP and gRPC protocols in OTLP handler
- Token-based authentication for secure OTLP endpoints
- Custom log levels for third-party libraries

### Quick Start

#### Basic Usage

```python
from recovery_libs.utils import logger

# Simple logging
logger.info("Application started")
logger.warning("High memory usage detected")
logger.error("Database connection failed")

# With structured data
logger.info("User login successful", extra={"user_id": 123, "ip_address": "192.168.1.1"})
```

#### Custom Configuration

```python
from recovery_libs.utils import get_logger
import logging

# Create logger with custom settings
logger = get_logger(
    name="myapp",
    level=logging.DEBUG,
    custom_library_level={
        "httpx": logging.WARNING,
        "urllib3": logging.ERROR,
        "sqlalchemy": logging.WARNING,
    },
)

logger.debug("Debug information")
logger.info("Application event")
```

#### Structured Data with Context

```python
from recovery_libs.utils import logger

# Simulate processing a request
request_id = "req-abc-123"
user_id = 789

logger.info(
    "Processing API request",
    extra={
        "request_id": request_id,
        "user_id": user_id,
        "endpoint": "/api/v1/users",
        "method": "GET",
    },
)

# After processing
logger.info(
    "API request completed",
    extra={
        "request_id": request_id,
        "user_id": user_id,
        "duration_ms": 100,
        "status_code": 200,
    },
)
```

#### Error Handling with Exception Info

```python
from recovery_libs.utils import logger

try:
    # Some operation that may fail
    result = 1 / 0
except ZeroDivisionError as e:
    logger.error(
        "Division by zero error",
        extra={
            "operation": "divide",
            "numerator": 1,
            "denominator": 0,
        },
        exc_info=True,  # Include exception traceback
    )
```

### Best Practices

#### 1. Use Structured Data

```python
# Good - structured data
logger.info("User action", extra={"user_id": 123, "action": "purchase", "amount": 99.99})

# Bad - unstructured
logger.info(f"User 123 purchased item for $99.99")
```

#### 2. Suppress Third-Party Noise

```python
# Reduce log volume in production
logger = get_logger(
    level=logging.INFO,
    custom_library_level={
        "httpx": logging.WARNING,
        "urllib3": logging.ERROR,
    },
)
```

#### 3. Include Context in Logs

```python
# Add correlation IDs, user context, etc.
logger.info(
    "API request completed",
    extra={
        "request_id": request_id,
        "user_id": user_id,
        "endpoint": "/api/users",
        "duration_ms": 145,
        "status_code": 200,
    },
)
```

#### 4. Cache Logger Instance at Module Level

```python
# Good - create once, reuse everywhere
from recovery_libs.utils import get_logger

logger = get_logger()  # Called once at module import


def my_function():
    logger.info("Processing")
```
