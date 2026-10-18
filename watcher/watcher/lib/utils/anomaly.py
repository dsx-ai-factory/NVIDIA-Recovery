# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from collections import deque
from datetime import UTC, datetime, timedelta

import numpy as np

from watcher.lib.utils.logging import logger


class IterationThroughputMonitor:
    """
    Monitors iteration throughput (e.g., TFLOPs per iteration) over time
    and detects degrading anomalies based on percentile-based thresholds.

    Example:
        >>> from datetime import datetime, timedelta
        >>> monitor = IterationThroughputMonitor(
        ...     window_size=100,
        ...     degradation_percentile_threshold=10.0,
        ...     alert_smoothing_count=2,
        ...     warmup_grace_period_secs=300,
        ...     min_window_size_for_detection=30,
        ...     min_interval_between_datapoints_secs=60,
        ...     skip_too_close_datapoints=True
        ... )
        >>> now = datetime.utcnow()
        >>> monitor.add(350.0, timestamp=now)
        >>> monitor.add(300.0, timestamp=now + timedelta(seconds=120))
        >>> monitor.get_history()
        [350.0, 300.0]
    """

    def __init__(
        self,
        window_size: int = 100,
        base_percentile_threshold: float = 35.0,
        anomaly_percent_threshold: float = 10.0,
        alert_smoothing_count: int = 5,
        warmup_grace_period_secs: int = 600,
        min_window_size_for_detection: int = 10,
        min_interval_between_datapoints_secs: int = 30,
        skip_too_close_datapoints: bool = False,
        reset_after_good_values: int | None = None,
        notify_once: bool = False,
    ):
        """
        Args:
            window_size: Max datapoints retained.
            base_percentile_threshold: Base percentile (e.g., 10 for 10th and 90th).
            anomaly_percent_threshold: Buffer percentage around cutoffs.
            alert_smoothing_count: Alerts only after N anomalies.
            warmup_grace_period_secs: Ignore anomalies during warmup.
            min_window_size_for_detection: Minimum history size before detection.
            min_interval_between_datapoints_secs: Minimum time between datapoints.
            skip_too_close_datapoints: Skip datapoints if added too soon.
            reset_after_good_values: Reset degradation counter after N good values (defaults to min_window_size).
            notify_once: When True, prevents duplicate notifications for the same anomaly episode.
        """
        if min_window_size_for_detection > window_size:
            raise ValueError("min_window_size_for_detection cannot be greater than window_size")

        self.window_size = window_size
        self.base_percentile_threshold = base_percentile_threshold
        self.anomaly_percent_threshold = anomaly_percent_threshold
        self.alert_smoothing_count = alert_smoothing_count
        self.warmup_grace_period = timedelta(seconds=warmup_grace_period_secs)
        self.min_window_size = min_window_size_for_detection
        self.min_interval = timedelta(seconds=min_interval_between_datapoints_secs)
        self.skip_too_close_datapoints = skip_too_close_datapoints
        self.reset_after_good_values = reset_after_good_values or min_window_size_for_detection

        self.history: deque[float] = deque(maxlen=window_size)
        self.timestamps: deque[datetime] = deque(maxlen=window_size)
        self.degradation_counter = 0
        self.good_value_counter = 0
        self.start_time: datetime | None = None
        self.last_update_time: datetime | None = None
        # Notification deduplication
        self.notify_once_enabled = notify_once
        self.notify_once_flag = False

    def add_and_validate(self, value: float, timestamp: datetime | None = None) -> bool:
        """
        Adds a new throughput datapoint and checks for anomaly.

        Args:
            value: The new datapoint (e.g., TFLOPs).
            timestamp: Optional timestamp. Defaults to datetime.utcnow().

        Returns:
            bool: True if anomaly alert is triggered.
        """
        # Ensure value is numeric
        if not isinstance(value, (int, float)):
            try:
                value = float(value)
            except (ValueError, TypeError) as e:
                logger.error(
                    f"[IterationThroughputMonitor] received non-numeric value: {value} (type: {type(value)}). Error: {e}"
                )
                return False

        if timestamp is None:
            timestamp = datetime.now(UTC)

        if self.start_time is None:
            self.start_time = timestamp

        # Enforce min interval
        if (
            self.min_interval.total_seconds() > 0
            and self.last_update_time is not None
            and ((timestamp <= self.last_update_time) or ((timestamp - self.last_update_time) < self.min_interval))
        ):
            if not self.skip_too_close_datapoints:
                self.history.append(value)
                self.timestamps.append(timestamp)

            return False

        self.last_update_time = timestamp
        self.history.append(value)
        self.timestamps.append(timestamp)

        if len(self.history) <= self.min_window_size:
            return False

        if timestamp is None or self.start_time is None:
            return False
        if (timestamp - self.start_time) < self.warmup_grace_period:
            return False

        # Discard current datapoint (value) from percentile cutoffs calculation
        if len(self.history) <= 1:
            return False

        eval_history = list(self.history)[:-1]
        if len(eval_history) < self.min_window_size:
            return False

        # Calculate percentile cutoffs
        low_cutoff = np.percentile(eval_history, self.base_percentile_threshold)
        high_cutoff = np.percentile(eval_history, 100 - self.base_percentile_threshold)

        low_cutoff_adjusted = low_cutoff - (low_cutoff * self.anomaly_percent_threshold / 100)
        high_cutoff_adjusted = high_cutoff + (high_cutoff * self.anomaly_percent_threshold / 100)

        # Evaluate current datapoint
        if value < low_cutoff_adjusted or value > high_cutoff_adjusted:
            self.degradation_counter += 1
            self.good_value_counter = 0
        else:
            self.good_value_counter += 1
            if self.good_value_counter >= self.reset_after_good_values:
                self.degradation_counter = 0
                # Reset notify_once only after sustained stability
                if self.notify_once_enabled and self.good_value_counter >= (self.reset_after_good_values * 2):
                    logger.debug(
                        f"[IterationThroughputMonitor] Resetting notify_once flag after sustained stability "
                        f"(good_value_counter={self.good_value_counter}, required={self.reset_after_good_values * 2})"
                    )
                    self.notify_once_flag = False

        # additional logging for troubleshooting
        logger.debug(f"[IterationThroughputMonitor] history: {list(self.history)[:-1]}")
        logger.debug(
            f"[IterationThroughputMonitor] adding value={value:.2f}, base_low_cutoff={low_cutoff:.2f}, "
            f"low_cutoff_adjusted={low_cutoff_adjusted:.2f}, base_high_cutoff={high_cutoff:.2f}, "
            f"high_cutoff_adjusted={high_cutoff_adjusted:.2f}, self.degradation_counter={self.degradation_counter}, "
            f"self.good_value_counter={self.good_value_counter}"
        )

        anomaly_evalution = self.degradation_counter >= self.alert_smoothing_count

        # Handle notify_once logic
        if self.notify_once_enabled and anomaly_evalution:
            if self.notify_once_flag:
                # Suppress duplicate notification
                logger.info(
                    f"[IterationThroughputMonitor] Suppressing duplicate notification "
                    f"(notify_once active, degradation_counter={self.degradation_counter})"
                )
                return False
            else:
                # First notification for this anomaly episode - activate flag
                logger.info(
                    f"[IterationThroughputMonitor] Activating notify_once flag for new anomaly episode "
                    f"(degradation_counter={self.degradation_counter})"
                )
                self.notify_once_flag = True

        if anomaly_evalution:
            logger.info(f"[IterationThroughputMonitor] history: {list(self.history)[:-1]}")
            logger.info(
                f"[IterationThroughputMonitor] adding value={value:.2f}, base_low_cutoff={low_cutoff:.2f}, "
                f"low_cutoff_adjusted={low_cutoff_adjusted:.2f}, base_high_cutoff={high_cutoff:.2f}, "
                f"high_cutoff_adjusted={high_cutoff_adjusted:.2f}, self.degradation_counter={self.degradation_counter}, "
                f"self.good_value_counter={self.good_value_counter}"
            )

        return anomaly_evalution

    def get_history(self) -> list:
        """
        Returns:
            list: The list of recent throughput values.
        """
        return list(self.history)

    def get_history_with_timestamps(self) -> list:
        """
        Returns:
            list of tuple: List of (timestamp, value) pairs.
        """
        return list(zip(self.timestamps, self.history))

    @property
    def stats(self) -> str:
        """
        Returns:
            str: Statistics about current state.
        """
        if len(self.history) < 1:
            return ""

        # Calculate percentile cutoffs
        low_cutoff = np.percentile(self.history, self.base_percentile_threshold)
        high_cutoff = np.percentile(self.history, 100 - self.base_percentile_threshold)

        low_cutoff_adjusted = low_cutoff - (low_cutoff * self.anomaly_percent_threshold / 100)
        high_cutoff_adjusted = high_cutoff + (high_cutoff * self.anomaly_percent_threshold / 100)

        return str(
            f"base_low_cutoff={low_cutoff:.2f}, "
            f"low_cutoff_adjusted={low_cutoff_adjusted:.2f}, base_high_cutoff={high_cutoff:.2f}, "
            f"high_cutoff_adjusted={high_cutoff_adjusted:.2f}, self.degradation_counter={self.degradation_counter}, "
            f"self.good_value_counter={self.good_value_counter}, history: {list(self.history)}"
        )
