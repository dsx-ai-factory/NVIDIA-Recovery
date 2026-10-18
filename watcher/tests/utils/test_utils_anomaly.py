# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timedelta

import pytest
from watcher.lib.utils.anomaly import IterationThroughputMonitor


@pytest.fixture
def base_time():
    return datetime(2025, 1, 1, 12, 0, 0)


def test_history_tracking(base_time):
    monitor = IterationThroughputMonitor(window_size=5, min_window_size_for_detection=2)
    monitor.add_and_validate(350.0, timestamp=base_time)
    assert monitor.get_history() == [350.0]
    assert len(monitor.get_history_with_timestamps()) == 1


def test_anomaly_detection(base_time):
    monitor = IterationThroughputMonitor(
        window_size=10,
        base_percentile_threshold=25.0,
        anomaly_percent_threshold=5.0,
        alert_smoothing_count=2,
        warmup_grace_period_secs=0,
        min_window_size_for_detection=5,
    )
    for i in range(5):
        monitor.add_and_validate(350.0, timestamp=base_time + timedelta(seconds=i * 60))
    assert not monitor.add_and_validate(300.0, timestamp=base_time + timedelta(seconds=300))
    assert monitor.add_and_validate(295.0, timestamp=base_time + timedelta(seconds=360))


def test_reset_after_good_values(base_time):
    monitor = IterationThroughputMonitor(
        alert_smoothing_count=3, reset_after_good_values=2, warmup_grace_period_secs=0, min_window_size_for_detection=5
    )
    for i in range(5):
        monitor.add_and_validate(350.0, timestamp=base_time + timedelta(seconds=i * 60))

    # Two anomalies - won't fire as alert_smoothing_count is set to 3
    assert not monitor.add_and_validate(300.0, timestamp=base_time + timedelta(seconds=300))
    assert not monitor.add_and_validate(300.0, timestamp=base_time + timedelta(seconds=360))

    # Two good values to reset
    monitor.add_and_validate(350.0, timestamp=base_time + timedelta(seconds=420))
    monitor.add_and_validate(351.0, timestamp=base_time + timedelta(seconds=480))

    # New anomaly starts fresh
    assert not monitor.add_and_validate(300.0, timestamp=base_time + timedelta(seconds=540))


def test_skip_too_close_true(base_time):
    monitor = IterationThroughputMonitor(min_interval_between_datapoints_secs=60, skip_too_close_datapoints=True)
    monitor.add_and_validate(350.0, timestamp=base_time)
    monitor.add_and_validate(351.0, timestamp=base_time + timedelta(seconds=30))
    assert len(monitor.get_history()) == 1  # second one was skipped


def test_skip_too_close_false(base_time):
    monitor = IterationThroughputMonitor(
        alert_smoothing_count=1,
        min_interval_between_datapoints_secs=60,
        warmup_grace_period_secs=0,
        min_window_size_for_detection=1,
        skip_too_close_datapoints=False,
    )
    assert not monitor.add_and_validate(350.0, timestamp=base_time)
    assert not monitor.add_and_validate(300.0, timestamp=base_time + timedelta(seconds=30))
    assert len(monitor.get_history()) == 2  # second one added, but ignored for alerting
    assert monitor.add_and_validate(250.0, timestamp=base_time + timedelta(seconds=91))  # should fire anomaly
    assert len(monitor.get_history()) == 3  # third one added


def test_features_basic_detection_percentile_bounds():
    monitor = IterationThroughputMonitor(
        window_size=5,
        base_percentile_threshold=25.0,
        anomaly_percent_threshold=10.0,
        alert_smoothing_count=1,
        min_window_size_for_detection=3,
        warmup_grace_period_secs=0,  # Disable warmup grace period
        min_interval_between_datapoints_secs=0,  # Disable minimum interval between throughput updates
    )

    monitor.add_and_validate(350)  # Normal
    monitor.add_and_validate(355)  # Normal
    monitor.add_and_validate(360)  # Normal

    # Current values: [350, 355, 360]
    # P25 ≈ 352.5, P75 ≈ 357.5
    # Acceptable bounds: [P25 - 10%, P75 + 10%] ≈ [317.25, 393.25]

    assert not monitor.add_and_validate(345)  # Within bounds → Not anomalous
    assert monitor.add_and_validate(300)  # Outside bounds → Anomaly!


def test_features_anomaly_threshold_count():
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


def test_features_warmup_grace_period():
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


def test_features_minimum_time_between_datapoints():
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


def test_features_reset_anomaly_counter_after_n_good_values():
    monitor = IterationThroughputMonitor(
        alert_smoothing_count=2,
        reset_after_good_values=2,
        min_window_size_for_detection=3,
        warmup_grace_period_secs=0,  # Disable warmup grace period
        min_interval_between_datapoints_secs=0,  # Disable minimum interval between throughput updates
        notify_once=False,  # Disable notify_once to test raw degradation counter reset behavior
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


## notify_once Tests
##


def test_notify_once_basic_functionality(base_time):
    """Test basic notify_once functionality - suppress duplicate notifications."""
    monitor = IterationThroughputMonitor(
        alert_smoothing_count=2,
        min_window_size_for_detection=3,
        warmup_grace_period_secs=0,
        min_interval_between_datapoints_secs=0,
        notify_once=True,
    )

    # Setup baseline values
    monitor.add_and_validate(350, timestamp=base_time)
    monitor.add_and_validate(355, timestamp=base_time + timedelta(seconds=60))
    monitor.add_and_validate(360, timestamp=base_time + timedelta(seconds=120))

    # First anomaly episode - should trigger notification after smoothing count
    assert not monitor.add_and_validate(300, timestamp=base_time + timedelta(seconds=180))  # 1st anomaly
    assert monitor.add_and_validate(290, timestamp=base_time + timedelta(seconds=240))  # 2nd anomaly → notification

    # Additional anomalies in same episode - should be suppressed
    assert not monitor.add_and_validate(280, timestamp=base_time + timedelta(seconds=300))  # 3rd anomaly → suppressed
    assert not monitor.add_and_validate(270, timestamp=base_time + timedelta(seconds=360))  # 4th anomaly → suppressed


def test_notify_once_disabled(base_time):
    """Test that disabling notify_once allows all notifications through."""
    monitor = IterationThroughputMonitor(
        alert_smoothing_count=2,
        min_window_size_for_detection=3,
        warmup_grace_period_secs=0,
        min_interval_between_datapoints_secs=0,
        notify_once=False,
    )

    # Setup baseline values
    monitor.add_and_validate(350, timestamp=base_time)
    monitor.add_and_validate(355, timestamp=base_time + timedelta(seconds=60))
    monitor.add_and_validate(360, timestamp=base_time + timedelta(seconds=120))

    # All anomalies should trigger notifications (after smoothing count)
    assert not monitor.add_and_validate(300, timestamp=base_time + timedelta(seconds=180))  # 1st anomaly
    assert monitor.add_and_validate(290, timestamp=base_time + timedelta(seconds=240))  # 2nd anomaly → notification
    assert monitor.add_and_validate(
        280, timestamp=base_time + timedelta(seconds=300)
    )  # 3rd anomaly → notification (not suppressed)
    assert monitor.add_and_validate(
        270, timestamp=base_time + timedelta(seconds=360)
    )  # 4th anomaly → notification (not suppressed)


def test_notify_once_reset_after_sustained_stability(base_time):
    """Test that notify_once flag resets after sustained stability (2x reset_after_good_values)."""
    monitor = IterationThroughputMonitor(
        alert_smoothing_count=2,
        reset_after_good_values=3,
        min_window_size_for_detection=3,
        warmup_grace_period_secs=0,
        min_interval_between_datapoints_secs=0,
        notify_once=True,
    )

    # Setup baseline values
    monitor.add_and_validate(350, timestamp=base_time)
    monitor.add_and_validate(355, timestamp=base_time + timedelta(seconds=60))
    monitor.add_and_validate(360, timestamp=base_time + timedelta(seconds=120))

    # First anomaly episode
    assert not monitor.add_and_validate(300, timestamp=base_time + timedelta(seconds=180))  # 1st anomaly
    assert monitor.add_and_validate(290, timestamp=base_time + timedelta(seconds=240))  # 2nd anomaly → notification
    assert not monitor.add_and_validate(280, timestamp=base_time + timedelta(seconds=300))  # 3rd anomaly → suppressed

    # Add good values to trigger reset - need 2 * reset_after_good_values = 6 good values
    monitor.add_and_validate(355, timestamp=base_time + timedelta(seconds=360))  # Good 1
    monitor.add_and_validate(360, timestamp=base_time + timedelta(seconds=420))  # Good 2
    monitor.add_and_validate(358, timestamp=base_time + timedelta(seconds=480))  # Good 3 → degradation_counter = 0
    monitor.add_and_validate(362, timestamp=base_time + timedelta(seconds=540))  # Good 4
    monitor.add_and_validate(357, timestamp=base_time + timedelta(seconds=600))  # Good 5
    monitor.add_and_validate(359, timestamp=base_time + timedelta(seconds=660))  # Good 6 → notify_once_flag = False

    # New anomaly episode should allow notification again
    assert not monitor.add_and_validate(300, timestamp=base_time + timedelta(seconds=720))  # 1st anomaly (new episode)
    assert monitor.add_and_validate(
        290, timestamp=base_time + timedelta(seconds=780)
    )  # 2nd anomaly → notification (allowed)
    assert not monitor.add_and_validate(
        280, timestamp=base_time + timedelta(seconds=840)
    )  # 3rd anomaly → suppressed again


def test_notify_once_partial_stability_no_reset(base_time):
    """Test that notify_once flag does NOT reset with partial stability (< 2x reset_after_good_values)."""
    monitor = IterationThroughputMonitor(
        alert_smoothing_count=2,
        reset_after_good_values=3,
        min_window_size_for_detection=3,
        warmup_grace_period_secs=0,
        min_interval_between_datapoints_secs=0,
        notify_once=True,
    )

    # Setup baseline values
    monitor.add_and_validate(350, timestamp=base_time)
    monitor.add_and_validate(355, timestamp=base_time + timedelta(seconds=60))
    monitor.add_and_validate(360, timestamp=base_time + timedelta(seconds=120))

    # First anomaly episode
    assert not monitor.add_and_validate(300, timestamp=base_time + timedelta(seconds=180))  # 1st anomaly
    assert monitor.add_and_validate(290, timestamp=base_time + timedelta(seconds=240))  # 2nd anomaly → notification

    # Add only partial good values (5 < 6 required)
    monitor.add_and_validate(355, timestamp=base_time + timedelta(seconds=300))  # Good 1
    monitor.add_and_validate(360, timestamp=base_time + timedelta(seconds=360))  # Good 2
    monitor.add_and_validate(358, timestamp=base_time + timedelta(seconds=420))  # Good 3 → degradation_counter = 0
    monitor.add_and_validate(362, timestamp=base_time + timedelta(seconds=480))  # Good 4
    monitor.add_and_validate(357, timestamp=base_time + timedelta(seconds=540))  # Good 5 (still < 6 required)

    # Another anomaly - should still be suppressed (flag not reset)
    assert not monitor.add_and_validate(300, timestamp=base_time + timedelta(seconds=600))  # 1st anomaly (same episode)
    assert not monitor.add_and_validate(290, timestamp=base_time + timedelta(seconds=660))  # 2nd anomaly → suppressed


def test_notify_once_integration_with_smoothing(base_time):
    """Test notify_once works correctly with alert smoothing logic."""
    monitor = IterationThroughputMonitor(
        alert_smoothing_count=3,  # Require 3 anomalies before notification
        min_window_size_for_detection=3,
        warmup_grace_period_secs=0,
        min_interval_between_datapoints_secs=0,
        notify_once=True,
    )

    # Setup baseline values
    monitor.add_and_validate(350, timestamp=base_time)
    monitor.add_and_validate(355, timestamp=base_time + timedelta(seconds=60))
    monitor.add_and_validate(360, timestamp=base_time + timedelta(seconds=120))

    # First anomaly episode - need 3 anomalies for notification
    assert not monitor.add_and_validate(300, timestamp=base_time + timedelta(seconds=180))  # 1st anomaly
    assert not monitor.add_and_validate(290, timestamp=base_time + timedelta(seconds=240))  # 2nd anomaly
    assert monitor.add_and_validate(280, timestamp=base_time + timedelta(seconds=300))  # 3rd anomaly → notification

    # Additional anomalies should be suppressed by notify_once
    assert not monitor.add_and_validate(270, timestamp=base_time + timedelta(seconds=360))  # 4th anomaly → suppressed
    assert not monitor.add_and_validate(260, timestamp=base_time + timedelta(seconds=420))  # 5th anomaly → suppressed


def test_notify_once_edge_case_immediate_good_values(base_time):
    """Test edge case where good values come immediately after anomaly detection."""
    monitor = IterationThroughputMonitor(
        alert_smoothing_count=2,
        reset_after_good_values=2,
        min_window_size_for_detection=3,
        warmup_grace_period_secs=0,
        min_interval_between_datapoints_secs=0,
        notify_once=True,
    )

    # Setup baseline values
    monitor.add_and_validate(350, timestamp=base_time)
    monitor.add_and_validate(355, timestamp=base_time + timedelta(seconds=60))
    monitor.add_and_validate(360, timestamp=base_time + timedelta(seconds=120))

    # Quick anomaly followed by immediate recovery
    assert not monitor.add_and_validate(300, timestamp=base_time + timedelta(seconds=180))  # 1st anomaly
    assert monitor.add_and_validate(290, timestamp=base_time + timedelta(seconds=240))  # 2nd anomaly → notification

    # Immediate good values (degradation_counter resets but notify_once flag remains)
    monitor.add_and_validate(355, timestamp=base_time + timedelta(seconds=300))  # Good 1
    monitor.add_and_validate(360, timestamp=base_time + timedelta(seconds=360))  # Good 2 → degradation_counter = 0

    # Another anomaly burst - should still be suppressed
    assert not monitor.add_and_validate(300, timestamp=base_time + timedelta(seconds=420))  # 1st anomaly (same episode)
    assert not monitor.add_and_validate(290, timestamp=base_time + timedelta(seconds=480))  # 2nd anomaly → suppressed


def test_notify_once_flag_state_tracking():
    """Test internal state tracking of notify_once flag."""
    monitor = IterationThroughputMonitor(
        alert_smoothing_count=1,
        reset_after_good_values=2,
        min_window_size_for_detection=3,
        warmup_grace_period_secs=0,
        min_interval_between_datapoints_secs=0,
        notify_once=True,
    )

    # Initially flag should be False (notifications allowed)
    assert monitor.notify_once_flag is False
    assert monitor.notify_once_enabled is True

    # Setup baseline values
    monitor.add_and_validate(350)
    monitor.add_and_validate(355)
    monitor.add_and_validate(360)

    # First anomaly - flag should be set to True after notification
    assert monitor.add_and_validate(290)  # Anomaly → notification → flag set
    assert monitor.notify_once_flag is True

    # Add enough good values to reset flag (4 = 2 * reset_after_good_values)
    monitor.add_and_validate(355)  # Good 1
    monitor.add_and_validate(360)  # Good 2 → degradation_counter = 0
    monitor.add_and_validate(358)  # Good 3
    monitor.add_and_validate(362)  # Good 4 → notify_once_flag = False
    assert monitor.notify_once_flag is False
