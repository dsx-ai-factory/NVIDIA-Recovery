# STRAGGLER

[`IterationThroughputMonitor`](watcher/lib/utils/anomaly.py) is a lightweight Python class for detecting STRAGGLER anomalies in iteration throughput metrics (e.g., TFLOPs) over time. It uses a rolling percentile-based approach with configurable smoothing, warm-up period, and time-based controls.

## Features

- Rolling window of recent throughput values (e.g., last 100 iterations)
- Percentile-based degradation detection (e.g., alert if value < 10th percentile)
- Alert smoothing (N consecutive degradation points required)
- Warm-up grace period to suppress early false positives
- Minimum interval enforcement between datapoints
- Option to skip or retain datapoints that arrive too soon

---

## Default Initialization Parameters in STRAGGLER workflow

| Parameter | Type | Default | Description |
|----------|------|---------|-------------|
| `window_size` | `int` | `100` | Number of recent values to store and evaluate against. |
| `base_percentile_threshold` | `float` | `35` | The percentile used to define lower and upper bounds for normal values (e.g., 35 → P35). |
| `anomaly_percent_threshold` | `float` | `10.0` | The percentage margin to adjust the bounds for more tolerant anomaly detection (e.g., 10% deviation below P25 or above P75). |
| `alert_smoothing_count` | `int` | `5` | Number of consecutive anomalies required before triggering an alert. Helps reduce noise and false positives. |
| `warmup_grace_period_secs` | `int` | `600` | Number of seconds to ignore anomaly detection after the monitor is instantiated, giving time for metrics to stabilize. |
| `min_window_size_for_detection` | `int` | `10` | Minimum number of values required before detection logic activates. |
| `min_interval_between_datapoints_secs` | `int` | `30` | Minimum number of seconds between consecutive datapoints. Values added too close together may be skipped or ignored. |
| `skip_too_close_datapoints` | `bool` | `False` | If `True`, datapoints added within `min_interval_between_datapoints_secs` are skipped entirely. If `False`, they're added to history but not used for anomaly evaluation. |
| `reset_after_good_values` | `Optional[int]` | `None` (defaults to `min_window_size_for_detection`) | After N non-anomalous values, the anomaly counter is reset. Prevents old anomalies from lingering. |
| `notify_once` | `bool` | `False` | When enabled, prevents duplicate notifications from being sent to RabbitMQ for the same anomaly condition. |

---

## Examples

Each example demonstrates one feature in isolation using at most 5 datapoints for clarity.

---

### 1. Basic Anomaly Detection (Percentile Bounds)

    ```python
    monitor = IterationThroughputMonitor(
        window_size=5,
        base_percentile_threshold=25.0,
        anomaly_percent_threshold=10.0,
        alert_smoothing_count=1,
        min_window_size_for_detection=3,
        warmup_grace_period_secs=0,  # Disable warmup grace period
        min_interval_between_datapoints_secs=0,  # Disable minimum interval between throughput updates
    )

    monitor.add_and_validate(350)  # Good datapoint
    monitor.add_and_validate(355)  # Good datapoint
    monitor.add_and_validate(360)  # Good datapoint

    # Current values: [350, 355, 360]
    # P25 ≈ 352.5, P75 ≈ 357.5
    # Acceptable bounds: [P25 - 10%, P75 + 10%] ≈ [317.25, 393.25]

    assert not monitor.add_and_validate(345)  # Within bounds → Not anomalous
    assert monitor.add_and_validate(300)  # Outside bounds → Anomaly!
    ```

### 2. Anomaly Threshold Count (alert_smoothing_count)

    ```python
    monitor = IterationThroughputMonitor(
        alert_smoothing_count=2,
        window_size=5,
        min_window_size_for_detection=3,
        warmup_grace_period_secs=0,  # Disable warmup grace period
        min_interval_between_datapoints_secs=0,  # Disable minimum interval between throughput updates
    )

    monitor.add_and_validate(350)
    monitor.add_and_validate(355)
    monitor.add_and_validate(360)

    assert not monitor.add_and_validate(300)  # 1st anomaly → no alert yet (alert_smoothing_count=2)
    assert monitor.add_and_validate(290)  # 2nd anomaly → alert triggered
    ```

### 3. Warmup Grace Period

    ```python
    monitor = IterationThroughputMonitor(
        warmup_grace_period_secs=600,
        window_size=5,
        min_window_size_for_detection=1,  # Set minimum size of history rolling window to 1
        min_interval_between_datapoints_secs=0,  # Disable minimum interval between throughput updates
    )

    t0 = datetime(2025, 1, 1, 12, 0, 0)

    # Within warmup period → will not trigger alerts
    assert not monitor.add_and_validate(350, timestamp=t0)
    assert not monitor.add_and_validate(300, timestamp=t0 + timedelta(seconds=300))  # Still inside 600s
    ```

### 4. Minimum Time Between Datapoints

    ```python
    monitor = IterationThroughputMonitor(
        min_interval_between_datapoints_secs=60,
        skip_too_close_datapoints=True,
        warmup_grace_period_secs=0,
        min_window_size_for_detection=3,
    )

    t0 = datetime(2025, 1, 1, 12, 0, 0)

    monitor.add_and_validate(350, timestamp=t0)
    monitor.add_and_validate(300, timestamp=t0 + timedelta(seconds=30))  # Discarded due to interval being too close
    assert monitor.get_history() == [350]  # Still only one value
    ```

### 5. Reset Anomaly Counter After Good Values

    ```python
    monitor = IterationThroughputMonitor(
        alert_smoothing_count=2,
        reset_after_good_values=2,
        min_window_size_for_detection=3,
        warmup_grace_period_secs=0,  # Disable warmup grace period
        min_interval_between_datapoints_secs=0,  # Disable minimum interval between throughput updates
    )

    monitor.add_and_validate(350)
    monitor.add_and_validate(355)
    monitor.add_and_validate(360)

    assert not monitor.add_and_validate(300)  # 1st anomaly → no alert yet (alert_smoothing_count=2)
    assert monitor.add_and_validate(301)  # Anomaly 2 → alert triggered

    monitor.add_and_validate(355)  # Good datapoint 1
    monitor.add_and_validate(354)  # Good datapoint 2 → Resets anomaly count

    assert not monitor.add_and_validate(300)  # Anomaly 1 again (fresh counter)
    assert monitor.add_and_validate(290)  # Anomaly 2 → now alert fires
    ```
