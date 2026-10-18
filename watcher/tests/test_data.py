# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import time
from datetime import datetime
from hashlib import sha256

import pytest
from watcher.lib.utils.data import (
    ActionCache,
    EventDeduplicator,
    LabelRemapper,
    RunningAverage,
    TimedCounter,
    apply_label_remapping,
    convert_datetime_to_timestamp_ns,
    convert_timestamp_to_datetime_fromtimestamp_format,
    extract_structured_data_from_text,
    find_matching_regex_workflows,
    generate_unique_cache_key,
    read_api_token_from_file,
)
from watcher.models import config
from watcher.models.workflow import WorkflowStepType


def test_read_api_token_from_environment(monkeypatch):
    monkeypatch.delenv("LOKI_API_KEY_PATH", raising=False)
    monkeypatch.setenv("LOKI_API_TOKEN", "Bearer test-token")

    assert read_api_token_from_file() == "Bearer test-token"


def test_read_api_token_from_file_takes_precedence(monkeypatch, tmp_path):
    token_file = tmp_path / "token"
    token_file.write_text("file-token\n")
    monkeypatch.setenv("LOKI_API_KEY_PATH", str(token_file))
    monkeypatch.setenv("LOKI_API_TOKEN", "environment-token")

    assert read_api_token_from_file() == "file-token"


def test_read_api_token_returns_none_when_unconfigured(monkeypatch):
    monkeypatch.delenv("LOKI_API_KEY_PATH", raising=False)
    monkeypatch.delenv("LOKI_API_TOKEN", raising=False)

    assert read_api_token_from_file() is None


@pytest.fixture
def timed_counter():
    return TimedCounter(timeout=5)  # Use a smaller timeout for testing purposes


@pytest.fixture
def action_cache():
    return ActionCache(max_size=10, timeout=1)  # Small cache with 1 second TTL for testing


def test_add_value():
    """
    Test adding an value to the TimedCounter.
    """
    counter = TimedCounter(timeout=1)
    counter.add("value1")
    assert len(counter) == 1
    assert sorted(counter.tolist()) == ["value1"]


def test_timeout():
    """
    Test the timeout functionality of the TimedCounter.
    """
    counter = TimedCounter(timeout=0.1)
    counter.add("value1")
    time.sleep(0.2)
    assert len(counter) == 0
    assert sorted(counter.tolist()) == []


def test_per_add_timeout_expires_scheduled_item():
    counter = TimedCounter(timeout=1)
    counter.add("long", timeout=0.2)
    counter.add("short", timeout=0.05)

    time.sleep(0.1)
    assert counter.tolist() == ["long"]


def test_multiple_values():
    """
    Test adding multiple values to the TimedCounter.
    """
    counter = TimedCounter(timeout=1)
    counter.add("value1")
    counter.add("value2")
    counter.add("value3")
    assert len(counter) == 3
    assert sorted(counter.tolist()) == ["value1", "value2", "value3"]


def test_thread_safety():
    """
    Test the thread safety of the TimedCounter.
    """
    counter = TimedCounter(timeout=0.1)

    def add_value(value):
        counter.add(value)

    import threading

    threads = []
    for i in range(5):
        t = threading.Thread(target=add_value, args=(f"value{i}",))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    assert len(counter) == 5
    assert sorted(counter.tolist()) == ["value0", "value1", "value2", "value3", "value4"]

    time.sleep(0.2)
    assert len(counter) == 0
    assert sorted(counter.tolist()) == []


def test_tolist_empty(timed_counter):
    assert timed_counter.tolist() == []


def test_tolist_with_items(timed_counter):
    timed_counter.add("item1")
    timed_counter.add("item2")
    assert timed_counter.tolist() == ["item1", "item2"]


def test_clear(timed_counter):
    timed_counter.add("item1")
    timed_counter.add("item2")
    assert len(timed_counter) == 2
    timed_counter.clear()
    assert len(timed_counter) == 0
    assert timed_counter.tolist() == []


def test_convert_dt_to_timestamp_ns_with_date():
    dt = datetime(2024, 6, 11)
    expected_timestamp = str(int(datetime(2024, 6, 11).timestamp() * 1e9))
    assert convert_datetime_to_timestamp_ns(dt) == expected_timestamp


def test_convert_dt_to_timestamp_ns_with_str():
    dt = "1718089200"  # This is equivalent to 2024-06-11 00:00:00 UTC
    expected_timestamp = str(int(datetime.fromtimestamp(float(dt)).timestamp() * 1e9))
    assert convert_datetime_to_timestamp_ns(dt) == expected_timestamp


def test_convert_dt_to_timestamp_ns_with_float():
    dt = 1718089200.0  # This is equivalent to 2024-06-11 00:00:00 UTC
    expected_timestamp = str(int(datetime.fromtimestamp(dt).timestamp() * 1e9))
    assert convert_datetime_to_timestamp_ns(dt) == expected_timestamp


def test_convert_dt_to_timestamp_ns_with_int():
    dt = 1718089200  # This is equivalent to 2024-06-11 00:00:00 UTC
    expected_timestamp = str(int(datetime.fromtimestamp(float(dt)).timestamp() * 1e9))
    assert convert_datetime_to_timestamp_ns(dt) == expected_timestamp


def test_convert_dt_to_timestamp_ns_with_invalid_type():
    with pytest.raises(TypeError):
        convert_datetime_to_timestamp_ns([])


def test_running_avg_initial_state():
    """
    Test that the initial state of RunningAverage is correct.
    """
    running_avg = RunningAverage()
    assert running_avg.count == 0
    assert running_avg.total_sum == 0.0


def test_running_avg_single_value():
    """
    Test that the average is correct with a single value.
    """
    running_avg = RunningAverage()
    avg = running_avg.update_average(10.0)
    assert avg == 10.0
    assert running_avg.count == 1
    assert running_avg.total_sum == 10.0


def test_running_avg_multiple_values():
    """
    Test that the average is updated correctly with multiple values.
    """
    running_avg = RunningAverage()
    values = [10, 20, 30, 40, 50]
    expected_averages = [10.0, 15.0, 20.0, 25.0, 30.0]

    for value, expected in zip(values, expected_averages):
        avg = running_avg.update_average(value)
        assert avg == expected


def test_running_avg_zero_values():
    """
    Test that zero values are handled correctly.
    """
    running_avg = RunningAverage()
    values = [0, 0, 0]
    expected_averages = [0.0, 0.0, 0.0]

    for value, expected in zip(values, expected_averages):
        avg = running_avg.update_average(value)
        assert avg == expected


def test_running_avg_negative_values():
    """
    Test that negative values are handled correctly.
    """
    running_avg = RunningAverage()
    values = [-10, -20, -30]
    expected_averages = [-10.0, -15.0, -20.0]

    for value, expected in zip(values, expected_averages):
        avg = running_avg.update_average(value)
        assert avg == expected


def test_running_avg_mixed_values():
    """
    Test that a mix of positive and negative values is handled correctly.
    """
    running_avg = RunningAverage()
    values = [10, -20, 30, -40, 50]
    expected_averages = [10.0, -5.0, 6.666666666666667, -5.0, 6.0]

    for value, expected in zip(values, expected_averages):
        avg = running_avg.update_average(value)
        assert pytest.approx(avg, 0.0001) == expected


def test_convert_timestamp_to_datetime_fromtimestamp_format_seconds_to_seconds():
    assert convert_timestamp_to_datetime_fromtimestamp_format("1632998780") == 1632998780.0


def test_convert_timestamp_to_datetime_fromtimestamp_format_milliseconds_to_seconds():
    assert convert_timestamp_to_datetime_fromtimestamp_format("1632998780123") == 1632998780.123


def test_convert_timestamp_to_datetime_fromtimestamp_format_microseconds_to_seconds():
    assert convert_timestamp_to_datetime_fromtimestamp_format("1632998780123456") == 1632998780.123456


def test_convert_timestamp_to_datetime_fromtimestamp_format_nanoseconds_to_seconds():
    assert convert_timestamp_to_datetime_fromtimestamp_format("1632998780123456789") == 1632998780.123456789


def test_convert_timestamp_to_datetime_fromtimestamp_format_int_input():
    assert convert_timestamp_to_datetime_fromtimestamp_format(1632998780) == 1632998780.0


def test_convert_timestamp_to_datetime_fromtimestamp_format_float_input():
    assert convert_timestamp_to_datetime_fromtimestamp_format(1632998780.123) == 1632998780.123


def test_generate_unique_cache_key_default_length():
    key_suffix = "test_cache_key_ID"
    result = generate_unique_cache_key(key_suffix)
    hash_part, suffix_part = result.split("_", 1)
    assert len(hash_part) == 8
    assert suffix_part == key_suffix


def test_generate_unique_cache_key_custom_length():
    key_suffix = "test_cache_key_ID"
    custom_length = 12
    result = generate_unique_cache_key(key_suffix, length=custom_length)
    hash_part, suffix_part = result.split("_", 1)
    assert len(hash_part) == custom_length
    assert suffix_part == key_suffix


def test_generate_unique_cache_key_empty_suffix():
    with pytest.raises(ValueError):
        key_suffix = ""
        _ = generate_unique_cache_key(key_suffix)


def test_generate_unique_cache_key_hash_content():
    key_suffix = "test_cache_key_ID"
    result = generate_unique_cache_key(key_suffix)
    expected_hash_prefix = sha256(key_suffix.encode("utf-8")).hexdigest()[:8]
    expected_result = f"{expected_hash_prefix}_{key_suffix}"

    assert result == expected_result


@pytest.mark.parametrize(
    "key_suffix, length",
    [
        ("test1", 4),
        ("another_test", 16),
        ("short", 2),
        ("suffix", 32),
    ],
)
def test_generate_unique_cache_key_parametrized(key_suffix, length):
    result = generate_unique_cache_key(key_suffix, length)
    hash_part, suffix_part = result.split("_", 1)
    assert len(hash_part) == length
    assert suffix_part == key_suffix


def test_extract_structured_data_single_event_single_regex():
    event = "[NestedRestarter] name=[InProcess|InJob] state=handling stage=starting"
    regexes = [r".*state=(?P<state>\w+).*stage=(?P<stage>\w+)"]
    result = extract_structured_data_from_text(data=event, regexes=regexes, stop_after_first_match=True)
    assert len(result) == 1
    assert result[0]["data"] == event
    assert result[0]["state"] == "handling"
    assert result[0]["stage"] == "starting"


def test_extract_structured_data_multiple_events_multiple_regexes():
    events = [
        "[NestedRestarter] name=[InProcess|InJob] state=handling stage=starting",
        "[NestedRestarter] name=[InProcess|InJob] healthcheck=failed",
        "Hello world!",
    ]
    regexes = [
        "^.*NestedRestarter.*state=(?P<state>\\w+)\\s+stage=(?P<stage>\\w+).*$",
        # 2nd regex won't match anything since stop_after_first_match is set to True
        "^.*NestedRestarter.*state=(?P<field1>\\w+)\\s+stage=(?P<field2>\\w+).*$",
        "^.*NestedRestarter.*name=\\[(?P<name>[^\\]]+)\\].*",
    ]
    result = extract_structured_data_from_text(data=events, regexes=regexes, stop_after_first_match=True)
    assert len(result) == 2
    assert result[0]["state"] == "handling"
    assert result[0]["stage"] == "starting"
    assert result[1]["name"] == "InProcess|InJob"


def test_extract_structured_data_single_event_multiple_regexes_extract_all_regexes():
    event = "[NestedRestarter] name=[InProcess|InJob] state=handling stage=starting"
    regexes = [
        "^.*NestedRestarter.*state=(?P<state>\\w+)\\s+stage=(?P<stage>\\w+).*$",
        "^.*NestedRestarter.*name=\\[(?P<name>[^\\]]+)\\].*",
    ]
    result = extract_structured_data_from_text(data=event, regexes=regexes, stop_after_first_match=False)
    assert len(result) == 1
    assert result[0]["state"] == "handling"
    assert result[0]["stage"] == "starting"
    assert result[0]["name"] == "InProcess|InJob"


def test_extract_structured_data_no_matching_events():
    events = ["Miles Davis", "Duke Ellington"]
    regexes = [r".*state=(?P<state>\w+).*stage=(?P<stage>\w+)"]
    result = extract_structured_data_from_text(data=events, regexes=regexes, stop_after_first_match=True)
    assert len(result) == 0


def test_extract_structured_data_empty_events():
    events = []
    regexes = [r".*state=(?P<state>\w+).*stage=(?P<stage>\w+)"]
    result = extract_structured_data_from_text(data=events, regexes=regexes, stop_after_first_match=True)
    assert len(result) == 0


def test_extract_structured_data_empty_regexes():
    event = "[NestedRestarter] name=[InProcess|InJob] state=handling stage=starting"
    result = extract_structured_data_from_text(data=event, regexes=[], stop_after_first_match=True)
    assert len(result) == 0


def make_test_config_for_regex_matching():
    attr1 = config.RuleAttributesConfig(query={"regex": ["abc", "123"]})
    attr2 = config.RuleAttributesConfig(query={"regex": ["xyz", "abc"]})
    child = config.RuleWorkflowConfig(name="child", type=WorkflowStepType.regex_parser, attributes=attr2)
    wf1 = config.RuleWorkflowConfig(name="wf1", type=WorkflowStepType.log_query, attributes=attr1, children=[child])
    wf2 = config.RuleWorkflowConfig(name="wf2", type=WorkflowStepType.log_query, attributes=attr2)
    rule = config.RuleConfig(name="r1", workflow=[wf1, wf2])
    watcher = config.WatcherConfig(
        datasources=config.DatasourcesConfig(
            log_query=config.LogQueryConfig.from_json(
                {
                    "providers": {
                        "loki": {
                            "url": "http://localhost:3100",
                            "queries": {"workflows": {"wf1": {"query": ["foo"]}, "wf2": {"query": ["bar"]}}},
                        }
                    }
                }
            ),
        ),
        event_source=config.WatcherEventSource.LOG_QUERY,
        rules=[rule],
    )
    return watcher, wf1, wf2, child


def test_find_matching_regex_workflows_single_match():
    watcher, *_ = make_test_config_for_regex_matching()
    matches = find_matching_regex_workflows(watcher, "abc")
    # wf1, wf2, and child all have 'abc' in their regex list
    names = {m["name"] for m in matches}
    assert {"wf1", "wf2", "child"} == names
    # All types should be present
    types = {m["type"] for m in matches}
    assert WorkflowStepType.log_query in types
    assert WorkflowStepType.regex_parser in types


def test_find_matching_regex_workflows_multiple_patterns():
    watcher, *_ = make_test_config_for_regex_matching()
    matches = find_matching_regex_workflows(watcher, "xyz")
    # Only wf2 and child have 'xyz' in their regex list
    names = {m["name"] for m in matches}
    assert {"wf2", "child"} == names


def test_find_matching_regex_workflows_no_match():
    watcher, *_ = make_test_config_for_regex_matching()
    matches = find_matching_regex_workflows(watcher, "notfound")
    assert matches == []


def test_find_matching_regex_workflows_no_duplicates():
    watcher, *_ = make_test_config_for_regex_matching()
    # 'abc' is in both wf1, wf2, and child, but should only appear once per (name, type)
    matches = find_matching_regex_workflows(watcher, "abc")
    seen = set()
    for m in matches:
        key = (m["name"], m["type"])
        assert key not in seen
        seen.add(key)


## ActionCache Tests
##


def test_action_cache_basic_functionality(action_cache):
    """Test basic ActionCache operations."""
    # Test add and get
    action_cache.add("key1", "value1")
    assert action_cache.get("key1") == "value1"
    assert len(action_cache) == 1

    # Test key exists
    assert "key1" in action_cache

    # Test string representation
    cache_str = str(action_cache)
    assert "key1" in cache_str


def test_action_cache_default_ttl_expiry(action_cache):
    """Test default TTL expiry functionality."""
    action_cache.add("key1", "value1")
    assert action_cache.get("key1") == "value1"

    # Wait for expiration (default 1 second in fixture)
    time.sleep(1.1)

    # Should be expired
    with pytest.raises(KeyError):
        action_cache.get("key1")

    assert len(action_cache) == 0


def test_action_cache_custom_ttl():
    """Test per-item TTL functionality."""
    cache = ActionCache(max_size=10, timeout=10)  # Default 10 seconds

    # Add item with custom short TTL
    cache.add("short_ttl", "value1", ttl=0.1)
    # Add item with default TTL
    cache.add("default_ttl", "value2")

    assert cache.get("short_ttl") == "value1"
    assert cache.get("default_ttl") == "value2"

    # Wait for short TTL to expire
    time.sleep(0.15)

    # Short TTL should be expired, default should still exist
    with pytest.raises(KeyError):
        cache.get("short_ttl")

    assert cache.get("default_ttl") == "value2"


def test_action_cache_list_merging():
    """Test list merging functionality."""
    cache = ActionCache(max_size=10, timeout=10)

    # Add initial list
    cache.add("alerts", ["alert1"])
    assert cache.get("alerts") == ["alert1"]

    # Add another list - should merge
    cache.add("alerts", ["alert2"])
    assert cache.get("alerts") == ["alert1", "alert2"]

    # Add single item to list - should append
    cache.add("alerts", "alert3")
    assert cache.get("alerts") == ["alert1", "alert2", "alert3"]


def test_action_cache_list_merging_with_ttl():
    """Test list merging with custom TTL."""
    cache = ActionCache(max_size=10, timeout=10)

    # Add initial list with custom TTL
    cache.add("notifications", ["notif1"], ttl=0.2)
    assert cache.get("notifications") == ["notif1"]

    # Add to the list with new TTL (should reset expiry)
    cache.add("notifications", ["notif2"], ttl=1.0)
    merged = cache.get("notifications")
    assert merged == ["notif1", "notif2"]

    # Wait past original TTL but not past new TTL
    time.sleep(0.3)

    # Should still exist due to new TTL
    assert cache.get("notifications") == ["notif1", "notif2"]


def test_action_cache_non_list_replacement():
    """Test that non-list values are replaced, not merged."""
    cache = ActionCache(max_size=10, timeout=10)

    # Add string value
    cache.add("config", "value1")
    assert cache.get("config") == "value1"

    # Replace with new string value (not a list)
    cache.add("config", "value2")
    assert cache.get("config") == "value2"

    # Replace with list
    cache.add("config", ["item1"])
    assert cache.get("config") == ["item1"]


def test_action_cache_delete():
    """Test cache deletion functionality."""
    cache = ActionCache(max_size=10, timeout=10)

    cache.add("key1", "value1")
    cache.add("key2", "value2")
    assert len(cache) == 2

    # Delete one key
    cache.delete("key1")
    assert len(cache) == 1

    # Check key is gone
    with pytest.raises(KeyError):
        cache.get("key1")

    # Other key should still exist
    assert cache.get("key2") == "value2"


def test_action_cache_ttl_method():
    """Test TTL querying functionality."""
    cache = ActionCache(max_size=10, timeout=10)

    # Add item with custom TTL
    cache.add("temp", "value", ttl=5.0)

    # Check TTL
    remaining = cache.ttl("temp")
    assert remaining is not None
    assert 4.0 < remaining <= 5.0

    # Add item with default TTL (ttl=None means use default_ttl)
    cache.add("with_default", "value", ttl=None)
    remaining_default = cache.ttl("with_default")
    assert remaining_default is not None
    assert 9.0 < remaining_default <= 10.0

    # Test permanent item (no TTL at all in VariableTTLLRUCache means need different approach)
    # For now, let's test that ttl=None uses the default_ttl as expected
    permanent_cache = ActionCache(max_size=10, timeout=None)  # No default TTL
    permanent_cache.add("permanent", "value", ttl=None)
    # This should use the default_ttl which is None, so no time expiry
    assert permanent_cache.ttl("permanent") is None

    # Check non-existent key raises KeyError
    with pytest.raises(KeyError):
        cache.ttl("nonexistent")


def test_action_cache_iteration():
    """Test cache iteration functionality."""
    cache = ActionCache(max_size=10, timeout=10)

    cache.add("key1", "value1")
    cache.add("key2", "value2")
    cache.add("key3", "value3")

    # Test iteration
    keys = list(cache)
    assert len(keys) == 3
    assert "key1" in keys
    assert "key2" in keys
    assert "key3" in keys


def test_action_cache_lru_eviction():
    """Test LRU eviction when cache reaches max size."""
    cache = ActionCache(max_size=2, timeout=10)  # Small cache

    cache.add("key1", "value1")
    cache.add("key2", "value2")
    assert len(cache) == 2

    # Access key1 to make it more recently used
    _ = cache.get("key1")

    # Add third item - should evict least recently used (key2)
    cache.add("key3", "value3")
    assert len(cache) == 2

    # key1 and key3 should exist, key2 should be evicted
    assert cache.get("key1") == "value1"
    assert cache.get("key3") == "value3"

    with pytest.raises(KeyError):
        cache.get("key2")


def test_action_cache_thread_safety():
    """Test ActionCache thread safety."""
    import threading

    cache = ActionCache(max_size=100, timeout=10)
    results = []
    exceptions = []

    def worker(thread_id):
        try:
            for i in range(10):
                key = f"thread_{thread_id}_key_{i}"
                cache.add(key, f"value_{i}")
                if key in cache:
                    value = cache.get(key)
                    results.append((thread_id, key, value))
        except Exception as e:  # noqa: BLE001
            exceptions.append(e)

    # Run multiple threads
    threads = []
    for i in range(5):
        thread = threading.Thread(target=worker, args=(i,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    # Should have no exceptions
    assert len(exceptions) == 0
    # Should have results from all threads
    assert len(results) > 0


def test_action_cache_backward_compatibility():
    """Test backward compatibility with old ActionCache interface."""
    # Test initialization without named parameters (positional args)
    cache = ActionCache(50, 300)  # max_size=50, timeout=300

    # Test add without TTL parameter (should work like before)
    cache.add("key1", "value1")
    assert cache.get("key1") == "value1"

    # Test list merging still works
    cache.add("lists", ["item1"])
    cache.add("lists", ["item2"])
    assert cache.get("lists") == ["item1", "item2"]


def test_event_deduplicator_signature_pattern():
    dedup = EventDeduplicator()
    sig = dedup.get_signature('{"msg": "error", "ts": 123}', signature_pattern=r'"msg":\s*"([^"]+)"')
    assert sig == "error"


def test_event_deduplicator_strip_patterns():
    dedup = EventDeduplicator()
    strip = [r"^\[\d{4}-\d{2}-\d{2}\s[\d:,]+\]\s*", r"\brank=\d+\s*"]
    line = "[2025-12-01 15:34:59,257] rank=329 [ALWAYS] state=aborted"
    assert dedup.get_signature(line, strip_patterns=strip) == "[ALWAYS] state=aborted"


def test_event_deduplicator_context_none_normalized():
    dedup = EventDeduplicator()
    ctx_with_none = {"workflow_step": "step", "routing_suffix": None, "job_id": None, "log_file_name": "r.log"}
    ctx_with_empty = {"workflow_step": "step", "routing_suffix": "", "job_id": "", "log_file_name": "r.log"}
    assert dedup.get_signature("body", context=ctx_with_none) == dedup.get_signature("body", context=ctx_with_empty)


def test_event_deduplicator_duplicate_detection_with_context():
    dedup = EventDeduplicator(window_seconds=60)
    ctx1, ctx2 = {"workflow_step": "step1", "job_id": "123"}, {"workflow_step": "step2", "job_id": "123"}
    assert [dedup.is_duplicate("same", context=c) for c in [ctx1, ctx2, ctx1]] == [False, False, True]


def test_event_deduplicator_ttl_expiry():
    dedup = EventDeduplicator(window_seconds=0.1)
    assert not dedup.is_duplicate("event1")
    assert dedup.is_duplicate("event1")
    time.sleep(0.15)
    assert not dedup.is_duplicate("event1")


def test_event_deduplicator_capacity_metrics():
    dedup = EventDeduplicator(max_size=100)
    assert dedup.size == 0 and dedup.max_size == 100 and dedup.utilization == 0.0
    dedup.is_duplicate("e1")
    dedup.is_duplicate("e2")
    assert dedup.size == 2 and dedup.utilization == 2.0


## LabelRemapper Tests
##


@pytest.mark.parametrize(
    "simple,composite,labels,expected",
    [
        # Simple remapping
        ({"job_id": "slurm_job_id"}, None, {"slurm_job_id": "123"}, {"job_id": "123"}),
        # Composite remapping
        (
            None,
            [{"attribute": "path", "template": "/log/{name}/{id}", "labels": ["name", "id"]}],
            {"name": "app", "id": "1"},
            {"path": "/log/app/1"},
        ),
        # Both simple and composite
        (
            {"job_id": "slurm_job_id"},
            [{"attribute": "path", "template": "{a}_{b}", "labels": ["a", "b"]}],
            {"slurm_job_id": "99", "a": "x", "b": "y"},
            {"job_id": "99", "path": "x_y"},
        ),
        # Missing label - skipped
        ({"job_id": "missing"}, None, {"other": "val"}, {}),
        # Composite missing required label - skipped
        (None, [{"attribute": "p", "template": "{a}_{b}", "labels": ["a", "b"]}], {"a": "x"}, {}),
    ],
)
def test_label_remapper_apply(simple, composite, labels, expected):
    remapper = LabelRemapper(simple=simple, composite=composite)
    assert remapper.apply(labels) == expected


def test_label_remapper_is_empty():
    assert LabelRemapper().is_empty
    assert not LabelRemapper(simple={"a": "b"}).is_empty
    assert not LabelRemapper(composite=[{"attribute": "x", "template": "{y}", "labels": ["y"]}]).is_empty


def test_apply_label_remapping_precedence():
    attrs = {"job_id": "old", "other": "keep"}
    remapper = LabelRemapper(simple={"job_id": "slurm_id"})
    result = apply_label_remapping(attrs, {"slurm_id": "new"}, remapper)
    assert result["job_id"] == "new" and result["other"] == "keep"


def test_label_remapper_warns_on_missing_simple_label(caplog):
    remapper = LabelRemapper(simple={"job_id": "missing_label"})
    with caplog.at_level(logging.WARNING):
        result = remapper.apply({"other": "val"})
    assert result == {}
    assert any("Simple remap attribute 'job_id'" in msg and "'missing_label'" in msg for msg in caplog.messages)


def test_label_remapper_warns_on_missing_composite_labels(caplog):
    remapper = LabelRemapper(composite=[{"attribute": "path", "template": "{a}_{b}", "labels": ["a", "b"]}])
    with caplog.at_level(logging.WARNING):
        result = remapper.apply({"a": "x"})
    assert result == {}
    assert any("Composite remap attribute 'path'" in msg and "['b']" in msg for msg in caplog.messages)
