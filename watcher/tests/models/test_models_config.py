# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pytest
import yaml
from recovery_libs.audit import AuditTrailSettings
from recovery_libs.log_query import BackendName
from watcher.models import config
from watcher.models.config import WatcherServiceConfig
from watcher.models.workflow import WorkflowStepType


# Pytest fixtures for common test data
@pytest.fixture
def default_config():
    """Fixture providing a default WatcherServiceConfig instance."""
    return WatcherServiceConfig()


@pytest.fixture
def custom_config():
    """Fixture providing a custom WatcherServiceConfig instance."""
    return WatcherServiceConfig(
        host="127.0.0.1",
        port=9090,
        http_log_level="DEBUG",
        max_queue_size=512,
        maintenance_tasks_interval_secs=60,
        num_threads=10,
        notification_dedup_ttl_secs=600,
    )


@pytest.fixture
def expected_defaults():
    """Fixture providing expected default values for assertions."""
    return {
        "host": "0.0.0.0",
        "port": 8080,
        "http_log_level": "WARNING",
        "max_queue_size": 1024,
        "maintenance_tasks_interval_secs": 30,
        "num_threads": 5,
        "notification_dedup_ttl_secs": 300,
    }


@pytest.fixture
def expected_custom():
    """Fixture providing expected custom values for assertions."""
    return {
        "host": "127.0.0.1",
        "port": 9090,
        "http_log_level": "DEBUG",
        "max_queue_size": 512,
        "maintenance_tasks_interval_secs": 60,
        "num_threads": 10,
        "notification_dedup_ttl_secs": 600,
    }


class TestWatcherServiceConfig:
    """Test suite for WatcherServiceConfig class."""

    @pytest.mark.unit
    def test_default_values(self, default_config, expected_defaults):
        """Test that default values are set correctly."""
        for field, expected_value in expected_defaults.items():
            assert getattr(default_config, field) == expected_value

    @pytest.mark.unit
    def test_custom_values(self, custom_config, expected_custom):
        """Test initialization with custom values."""
        for field, expected_value in expected_custom.items():
            assert getattr(custom_config, field) == expected_value

    @pytest.mark.unit
    def test_partial_custom_values(self, expected_defaults):
        """Test initialization with only some custom values."""
        config = WatcherServiceConfig(port=3000, notification_dedup_ttl_secs=900)

        # Custom values
        assert config.port == 3000
        assert config.notification_dedup_ttl_secs == 900

        # Default values should remain
        assert config.host == expected_defaults["host"]
        assert config.http_log_level == expected_defaults["http_log_level"]
        assert config.max_queue_size == expected_defaults["max_queue_size"]
        assert config.maintenance_tasks_interval_secs == expected_defaults["maintenance_tasks_interval_secs"]
        assert config.num_threads == expected_defaults["num_threads"]

    @pytest.mark.unit
    @pytest.mark.json
    def test_from_json_empty_dict(self, expected_defaults):
        """Test from_json with empty dictionary returns defaults."""
        config = WatcherServiceConfig.from_json({})

        for field, expected_value in expected_defaults.items():
            assert getattr(config, field) == expected_value

    @pytest.mark.unit
    @pytest.mark.json
    def test_from_json_partial_data(self, expected_defaults):
        """Test from_json with partial data."""
        data = {"port": 9999, "notification_dedup_ttl_secs": 1200}
        config = WatcherServiceConfig.from_json(data)

        # Provided values
        assert config.port == 9999
        assert config.notification_dedup_ttl_secs == 1200

        # Default values for missing fields
        assert config.host == expected_defaults["host"]
        assert config.http_log_level == expected_defaults["http_log_level"]
        assert config.max_queue_size == expected_defaults["max_queue_size"]
        assert config.maintenance_tasks_interval_secs == expected_defaults["maintenance_tasks_interval_secs"]
        assert config.num_threads == expected_defaults["num_threads"]

    @pytest.mark.unit
    @pytest.mark.json
    def test_from_json_audit_trail(self):
        service_config = WatcherServiceConfig.from_json(
            {
                "audit_trail": {
                    "destination": "http://heimdall-notifications:8080/v1/audit/events",
                    "flush_interval_seconds": 1,
                }
            }
        )

        assert isinstance(service_config.audit_trail, AuditTrailSettings)
        assert str(service_config.audit_trail.destination) == "http://heimdall-notifications:8080/v1/audit/events"
        assert service_config.audit_trail.flush_interval_seconds == 1

    @pytest.mark.unit
    @pytest.mark.json
    def test_from_json_complete_data(self):
        """Test from_json with complete data."""
        data = {
            "host": "localhost",
            "port": 8888,
            "http_log_level": "INFO",
            "max_queue_size": 2048,
            "maintenance_tasks_interval_secs": 120,
            "num_threads": 8,
            "notification_dedup_ttl_secs": 450,
        }
        config = WatcherServiceConfig.from_json(data)

        for field, expected_value in data.items():
            assert getattr(config, field) == expected_value

    @pytest.mark.unit
    @pytest.mark.json
    def test_from_json_with_none(self):
        """Test from_json with None returns None."""
        config = WatcherServiceConfig.from_json(None)
        assert config is None

    @pytest.mark.unit
    @pytest.mark.json
    def test_from_json_with_existing_instance(self):
        """Test from_json with existing WatcherServiceConfig instance."""
        original_config = WatcherServiceConfig(port=7777)
        config = WatcherServiceConfig.from_json(original_config)

        # Should return the same instance
        assert config is original_config
        assert config.port == 7777

    @pytest.mark.unit
    @pytest.mark.validation
    @pytest.mark.parametrize("ttl_value", [600, 0, 86400, 1])
    def test_notification_dedup_ttl_secs_values(self, ttl_value):
        """Test notification_dedup_ttl_secs with various valid values."""
        config = WatcherServiceConfig(notification_dedup_ttl_secs=ttl_value)
        assert config.notification_dedup_ttl_secs == ttl_value

    @pytest.mark.unit
    @pytest.mark.validation
    @pytest.mark.parametrize("port_value", [1, 65535, 80, 8080, 3000])
    def test_port_edge_cases(self, port_value):
        """Test port field with various valid values."""
        config = WatcherServiceConfig(port=port_value)
        assert config.port == port_value

    @pytest.mark.unit
    @pytest.mark.validation
    @pytest.mark.parametrize("thread_count", [1, 5, 10, 100])
    def test_num_threads_values(self, thread_count):
        """Test num_threads field with various valid values."""
        config = WatcherServiceConfig(num_threads=thread_count)
        assert config.num_threads == thread_count

    @pytest.mark.unit
    @pytest.mark.validation
    @pytest.mark.parametrize("log_level", ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    def test_http_log_level_values(self, log_level):
        """Test http_log_level with different valid log levels."""
        config = WatcherServiceConfig(http_log_level=log_level)
        assert config.http_log_level == log_level

    @pytest.mark.unit
    @pytest.mark.validation
    @pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "0.0.0.0", "192.168.1.100", "example.com"])
    def test_host_values(self, host):
        """Test host field with different valid host values."""
        config = WatcherServiceConfig(host=host)
        assert config.host == host

    @pytest.mark.unit
    def test_config_equality(self):
        """Test that two configs with same values are equal."""
        config1 = WatcherServiceConfig(host="localhost", port=8080, notification_dedup_ttl_secs=300)
        config2 = WatcherServiceConfig(host="localhost", port=8080, notification_dedup_ttl_secs=300)

        assert config1 == config2

    @pytest.mark.unit
    def test_config_inequality(self):
        """Test that two configs with different values are not equal."""
        config1 = WatcherServiceConfig(notification_dedup_ttl_secs=300)
        config2 = WatcherServiceConfig(notification_dedup_ttl_secs=600)

        assert config1 != config2

    @pytest.mark.unit
    def test_config_repr(self):
        """Test string representation of config."""
        config = WatcherServiceConfig(host="localhost", port=9000, notification_dedup_ttl_secs=450)

        repr_str = repr(config)
        expected_values = ["WatcherServiceConfig", "localhost", "9000", "450"]

        for expected_value in expected_values:
            assert str(expected_value) in repr_str

    @pytest.mark.unit
    @pytest.mark.regression
    def test_backwards_compatibility(self, expected_defaults):
        """Test that adding new field doesn't break existing functionality."""
        # Test that old configs without notification_dedup_ttl_secs still work
        data = {
            "host": "test.com",
            "port": 8080,
            "http_log_level": "DEBUG",
            "max_queue_size": 512,
            "maintenance_tasks_interval_secs": 45,
            "num_threads": 3,
            # Intentionally omitting notification_dedup_ttl_secs
        }

        config = WatcherServiceConfig.from_json(data)

        # All provided fields should be set
        for field, expected_value in data.items():
            assert getattr(config, field) == expected_value

        # New field should get default value
        assert config.notification_dedup_ttl_secs == expected_defaults["notification_dedup_ttl_secs"]


def test_watcher_event_source_enum():
    assert config.WatcherEventSource.LOG_QUERY.value == "log_query"
    assert config.WatcherEventSource.LOKI.value == "loki"
    assert config.WatcherEventSource.RABBITMQ.value == "rabbitmq"
    assert list(config.WatcherEventSource) == [
        config.WatcherEventSource.LOG_QUERY,
        config.WatcherEventSource.LOKI,
        config.WatcherEventSource.RABBITMQ,
    ]
    assert config.WatcherEventSource.LOG_QUERY.uses_log_query
    assert config.WatcherEventSource.LOKI.uses_log_query


def test_legacy_log_query_names_remain_accepted():
    watcher = config.WatcherConfig.from_json(
        {
            "datasources": {
                "loki": {},
                "log_query": {
                    "providers": {
                        "loki": {
                            "url": "http://localhost",
                            "queries": {"workflows": {"query": {"query": ['{job="app"}']}}},
                        }
                    }
                },
            },
            "event_source": "loki",
            "rules": [
                {
                    "name": "legacy",
                    "workflow": [
                        {
                            "name": "query",
                            "type": "loki_query",
                            "attributes": {},
                        }
                    ],
                }
            ],
        }
    )

    assert watcher.event_source == config.WatcherEventSource.LOKI
    assert watcher.rules[0].workflow[0].type == WorkflowStepType.loki_query


def test_log_query_policy_defaults_and_serialization():
    policy = config.LogQueryPolicyConfig()
    policy_json = policy.to_json()
    policy_copy = config.LogQueryPolicyConfig.from_json(policy_json)
    assert policy == policy_copy
    assert policy_json["url"] == ""


@pytest.mark.parametrize("policy_key", ["loki", "log_query_policy"])
@pytest.mark.parametrize("connection_key", ["url", "tenants", "org_id", "account_id", "project_id"])
def test_policy_rejects_legacy_connection_identity(policy_key, connection_key):
    with pytest.raises(ValueError, match="Move log-query"):
        config.DatasourcesConfig.from_json({policy_key: {connection_key: "legacy"}})


@pytest.mark.parametrize("backend", ["loki", "victorialogs"])
def test_selected_provider_connection_is_required(backend):
    with pytest.raises(ValueError, match=f"providers.{backend} is required"):
        config.LogQueryConfig.from_json({"backend": backend})


def test_polling_requires_explicit_provider_connection():
    with pytest.raises(ValueError, match="explicit provider connection is required"):
        config.WatcherConfig.from_json({"datasources": {"log_query_policy": {}}, "rules": []})


def test_unselected_provider_query_prefix_does_not_require_a_connection():
    selected = config.LogQueryConfig.from_json(
        {
            "backend": "victorialogs",
            "providers": {
                "loki": {"queries": {"query_prefix": ""}},
                "victorialogs": {"url": "http://victorialogs:9428"},
            },
        }
    )
    assert selected.providers.loki is None
    assert selected.selected_provider.url == "http://victorialogs:9428"


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("query_delay", -1, "query_delay must be a non-negative integer"),
        ("query_delay", True, "query_delay must be a non-negative integer"),
        ("query_max_lookback", 0, "query_max_lookback must be a positive integer"),
        ("query_max_lookback", "900", "query_max_lookback must be a positive integer"),
    ],
)
def test_log_query_policy_rejects_invalid_query_durations(field, value, message):
    with pytest.raises(ValueError, match=message):
        config.LogQueryPolicyConfig.from_json({field: value})


def test_log_query_config_defaults_to_loki_and_uses_provider_prefix():
    datasources = config.DatasourcesConfig(
        log_query=config.LogQueryConfig.from_json(
            {"providers": {"loki": {"url": "http://loki", "queries": {"query_prefix": '{job="app"}'}}}}
        ),
    )

    assert datasources.log_query_backend == BackendName.LOKI
    assert datasources.log_query_prefix == '{job="app"}'


def test_empty_loki_provider_prefix_is_not_replaced():
    datasources = config.DatasourcesConfig.from_json(
        {
            "log_query_policy": {},
            "log_query": {
                "backend": "loki",
                "providers": {"loki": {"url": "http://loki", "queries": {"query_prefix": ""}}},
            },
        }
    )

    assert datasources.log_query_prefix == ""


@pytest.mark.parametrize("policy_key", ["loki", "log_query_policy"])
def test_policy_query_prefix_is_rejected_even_when_empty(policy_key):
    with pytest.raises(ValueError, match="Move query_prefix"):
        config.DatasourcesConfig.from_json({policy_key: {"query_prefix": ""}})


@pytest.mark.parametrize("backend", ["loki", "victorialogs"])
def test_provider_top_level_query_prefix_is_rejected(backend):
    with pytest.raises(ValueError, match="Move provider query_prefix"):
        config.LogQueryConfig.from_json(
            {"backend": backend, "providers": {backend: {"url": "http://logs", "query_prefix": ""}}}
        )


def test_log_query_config_selects_victorialogs_provider():
    datasources = config.DatasourcesConfig.from_json(
        {
            "log_query_policy": {},
            "log_query": {
                "backend": "victorialogs",
                "providers": {
                    "victorialogs": {
                        "url": "http://victorialogs:9428",
                        "queries": {"query_prefix": "cluster:=slurm"},
                        "account_id": 1,
                        "project_id": 2,
                        "require_tenant": True,
                    }
                },
            },
        }
    )

    assert datasources.log_query_backend == BackendName.VICTORIALOGS
    assert datasources.log_query_prefix == "cluster:=slurm"
    assert datasources.log_query.selected_provider.account_id == 1


def _watcher_native_query_config(backend, native_queries, *, step_type="nooutput"):
    """Keep row templates in the selected provider while workflow policy stays neutral."""
    return {
        "datasources": {
            "log_query": {
                "backend": backend,
                "providers": {backend: {"url": "http://logs", "queries": {"workflows": native_queries}}},
            }
        },
        "rules": [{"name": "rule", "workflow": [{"name": "query", "type": step_type, "attributes": {}}]}],
    }


def test_victorialogs_selection_requires_provider():
    with pytest.raises(ValueError, match="providers.victorialogs is required"):
        config.LogQueryConfig.from_json({"backend": "victorialogs", "providers": {}})


@pytest.mark.parametrize("backend", ["loki", "victorialogs"])
@pytest.mark.parametrize("step_type", ["log_query", "loki_query", "nooutput"])
def test_selected_provider_requires_matching_workflow_name(backend, step_type):
    with pytest.raises(ValueError, match=rf"providers.{backend}.queries.workflows.query"):
        config.WatcherConfig.from_json(
            _watcher_native_query_config(backend, {"other": {"query": ["native-query"]}}, step_type=step_type)
        )


@pytest.mark.parametrize(
    "variant",
    [[], [""], ["   "], [{}], [{"TRAINING_LOOP": ""}], [{"TRAINING_LOOP": "   "}]],
)
def test_provider_native_query_must_be_nonempty_and_nonblank(variant):
    with pytest.raises(ValueError, match="Provider workflow query.*(empty|nonblank)"):
        config.WorkflowNativeQueries.from_json({"query": variant})


@pytest.mark.parametrize("validation_query", ["", "   "])
def test_selected_nooutput_validation_query_must_not_be_blank(validation_query):
    with pytest.raises(ValueError, match="nooutput validation query.*must not be blank"):
        config.WorkflowNativeQueries.from_json(
            {"query": ["_msg:* | sort by (_time)"], "nooutput_validation_query": validation_query}
        )


@pytest.mark.parametrize("backend", ["loki", "victorialogs"])
def test_nooutput_validation_query_requires_job_id_placeholder(backend):
    with pytest.raises(ValueError, match=r"must contain the literal placeholder \{job_id\}"):
        config.WatcherConfig.from_json(
            _watcher_native_query_config(
                backend,
                {"query": {"query": ["native-query"], "nooutput_validation_query": "missing-job-id"}},
            )
        )


@pytest.mark.parametrize("backend", ["loki", "victorialogs"])
def test_nooutput_requires_provider_validation_query(backend):
    with pytest.raises(ValueError, match="missing provider nooutput validation query"):
        config.WatcherConfig.from_json(_watcher_native_query_config(backend, {"query": {"query": ["native-query"]}}))


@pytest.mark.parametrize("backend", ["loki", "victorialogs"])
def test_provider_query_round_trip_preserves_order_and_phase_maps(backend):
    row_queries = [{"PRE_TRAINING": "first"}, "second", {"TRAINING_LOOP": "third"}, "fourth"]
    watcher = config.WatcherConfig.from_json(
        _watcher_native_query_config(
            backend,
            {"query": {"query": row_queries, "nooutput_validation_query": "job={job_id}"}},
        )
    )
    restored = config.WatcherConfig.from_json(watcher.to_json())
    assert restored.datasources.workflow_queries("query").query == row_queries
    assert restored.datasources.workflow_queries("query").nooutput_validation_query == "job={job_id}"


@pytest.mark.parametrize(
    ("step_type", "row_queries", "message"),
    [
        ("log_query", [{"TRAINING_LOOP": "native-query"}], "only accepts string row queries"),
        ("nooutput", [{"UNKNOWN": "native-query"}], "unknown nooutput query phase"),
    ],
)
def test_provider_workflow_phase_validation(step_type, row_queries, message):
    with pytest.raises(ValueError, match=message):
        config.WatcherConfig.from_json(
            _watcher_native_query_config(
                "loki",
                {"query": {"query": row_queries, "nooutput_validation_query": "job={job_id}"}},
                step_type=step_type,
            )
        )


@pytest.mark.parametrize("tenant_id", [-1, 4_294_967_296, True, "1"])
def test_log_query_provider_rejects_non_uint32_tenant_ids(tenant_id):
    with pytest.raises(ValueError, match="must be uint32"):
        config.LogQueryProviderConfig(
            url="http://victorialogs:9428",
            account_id=tenant_id,
            project_id=1,
        )


def test_log_query_provider_loads_decimal_string_tenant_ids_without_losing_zero():
    provider = config.LogQueryProviderConfig.from_json(
        {
            "url": "http://victorialogs:9428",
            "account_id": "12",
            "project_id": "0",
            "require_tenant": True,
        }
    )

    assert provider.account_id == 12
    assert provider.project_id == 0


def test_publisher_config_defaults_and_serialization():
    pub = config.RabbitMQPublisherConfig(
        host="http://rabbitmq_pub_url",
        username="guest",
        password="guest",
        vhost="/",
        exchange="watcher_eventbus_exchange_dev",
    )
    pub_json = pub.to_json()
    pub2 = config.RabbitMQPublisherConfig.from_json(pub_json)
    assert pub == pub2
    assert pub_json["queues"] == []
    assert pub_json["routing_keys"] == {}


def test_rabbitmq_subscriber_config_defaults_and_serialization():
    sub = config.RabbitMQSubscriberConfig(
        host="http://rabbitmq_pub_url",
        username="guest",
        password="guest",
        vhost="/",
        exchange_name="watcher_eventbus_exchange_dev",
        queue_name="application.logs",
        routing_key="application.logs",
    )
    sub_json = sub.to_json()
    sub2 = config.RabbitMQSubscriberConfig.from_json(sub_json)
    assert sub == sub2


def test_rabbitmq_config_nested_and_serialization():
    pub = config.RabbitMQPublisherConfig(
        host="host",
        username="guest",
        password="guest",
        vhost="/",
        exchange="watcher_eventbus_exchange_dev",
    )
    sub = config.RabbitMQSubscriberConfig(
        host="host2",
        username="guest",
        password="guest",
        vhost="/",
        exchange_name="watcher_eventbus_exchange_dev",
        queue_name="application.logs",
        routing_key="application.logs",
    )
    rabbit = config.RabbitMQConfig(publisher=pub, subscriber=sub)
    rabbit_json = rabbit.to_json()
    rabbit2 = config.RabbitMQConfig.from_json(rabbit_json)
    assert rabbit == rabbit2
    assert rabbit.publisher.host == "host"
    assert rabbit.subscriber.host == "host2"


def test_job_manager_config_and_serialization():
    jm = config.JobManagerConfig(endpoint="https://job_manager_url")
    jm_json = jm.to_json()
    jm2 = config.JobManagerConfig.from_json(jm_json)
    assert jm == jm2
    assert jm.endpoint == "https://job_manager_url"


def test_datasources_config_nested_and_serialization():
    policy = config.LogQueryPolicyConfig()
    pub = config.RabbitMQPublisherConfig(
        host="host",
        username="guest",
        password="guest",
        vhost="/",
        exchange="watcher_eventbus_exchange_dev",
    )
    rabbit = config.RabbitMQConfig(
        publisher=pub,
    )
    jm = config.JobManagerConfig(endpoint="https://job_manager_url")
    ds = config.DatasourcesConfig(log_query_policy=policy, rabbitmq=rabbit, job_manager=jm)
    ds_json = ds.to_json()
    ds2 = config.DatasourcesConfig.from_json(ds_json)
    assert ds == ds2
    assert ds.log_query_policy.url == ""
    assert ds.rabbitmq is rabbit
    assert ds.job_manager.endpoint == "https://job_manager_url"


def test_rule_action_config_and_serialization():
    action = config.RuleActionConfig(send_message_eventbus="anomaly.detected.HANG", unique=True)
    action_json = action.to_json()
    action2 = config.RuleActionConfig.from_json(action_json)
    assert action == action2
    assert action.send_message_eventbus == "anomaly.detected.HANG"
    assert action.unique is True


def test_rule_attributes_config_defaults_and_serialization():
    attr = config.RuleAttributesConfig()
    attr_json = attr.to_json()
    attr2 = config.RuleAttributesConfig.from_json(attr_json)
    assert attr == attr2
    assert attr.query is None
    assert attr.regex is None
    assert attr.cache_key is None
    assert attr.operation is None
    assert attr.anomaly_threshold is None
    assert attr.window_size is None
    assert attr.action is None
    assert attr.metric_name is None
    assert attr.metric_type is None
    assert attr.enable_metric_cleanup is None
    assert attr.interval is None
    assert attr.last_iteration_enabled is None
    assert attr.last_iteration_regex is None
    assert attr.last_iteration_operation_schema is None
    assert attr.false_positive_scan_cache_key is None
    assert attr.false_positive_scan_timer is None
    assert attr.log_timezone is None
    assert attr.events is None
    assert attr.send_message_eventbus is None


def test_rule_attributes_config_with_events():
    events = [
        config.EventRegex(event_type="type1", regexes=["regex1", "regex2"]),
        config.EventRegex(event_type="type2", regexes=["regex3"]),
    ]
    attr = config.RuleAttributesConfig(events=events)
    attr_json = attr.to_json()
    attr2 = config.RuleAttributesConfig.from_json(attr_json)
    assert attr == attr2
    assert len(attr.events) == 2
    assert attr.events[0].event_type == "type1"
    assert attr.events[0].regexes == ["regex1", "regex2"]
    assert attr.events[1].event_type == "type2"
    assert attr.events[1].regexes == ["regex3"]


def test_rule_attributes_config_with_eventbus():
    eventbus = config.EventBusConfig(routing_prefix="prefix", unique=True)
    attr = config.RuleAttributesConfig(send_message_eventbus=eventbus)
    attr_json = attr.to_json()
    attr2 = config.RuleAttributesConfig.from_json(attr_json)
    assert attr == attr2
    assert attr.send_message_eventbus.routing_prefix == "prefix"
    assert attr.send_message_eventbus.unique is True


def test_rule_attributes_config_query_schema():
    # Consumer-side regexes remain workflow attributes.
    attr = config.RuleAttributesConfig(query={"regex": ["foo", "bar"]})
    attr_json = attr.to_json()
    attr2 = config.RuleAttributesConfig.from_json(attr_json)
    assert attr == attr2
    assert attr.query == {"regex": ["foo", "bar"]}

    # Valid case: 'regex' with dicts for job phases
    attr = config.RuleAttributesConfig(query={"regex": ["baz", {"TRAINING_LOOP": "qux"}]})
    attr_json = attr.to_json()
    attr2 = config.RuleAttributesConfig.from_json(attr_json)
    assert attr == attr2
    assert attr.query == {"regex": ["baz", {"TRAINING_LOOP": "qux"}]}

    # Invalid key
    with pytest.raises(ValueError):
        config.RuleAttributesConfig.from_json({"query": {"badkey": ["foo"]}})

    # Invalid value type (not a list)
    with pytest.raises(ValueError):
        config.RuleAttributesConfig.from_json({"query": {"regex": "notalist"}})

    # Invalid value element type (not str or dict)
    with pytest.raises(ValueError):
        config.RuleAttributesConfig.from_json({"query": {"regex": [123]}})

    # Invalid dict element (not str->str)
    with pytest.raises(ValueError):
        config.RuleAttributesConfig.from_json({"query": {"regex": [{"PRE_TRAINING": 123}]}})


@pytest.mark.parametrize("backend", ["loki", "victorialogs"])
def test_rule_attributes_reject_native_queries(backend):
    with pytest.raises(ValueError, match="Move native workflow queries"):
        config.RuleAttributesConfig.from_json({"query": {backend: ["native-query"]}})


def test_rule_attributes_reject_nooutput_validation_query():
    with pytest.raises(ValueError, match="Move nooutput_validation_query"):
        config.RuleAttributesConfig.from_json({"nooutput_validation_query": {"loki": "job={job_id}"}})


def test_rule_workflow_config_nested_and_enum_and_serialization():
    attr = config.RuleAttributesConfig(
        query={"regex": ["re1", {"TRAINING_LOOP": "re2"}]},
        events=[config.EventRegex(event_type="type1", regexes=["regex1"])],
        send_message_eventbus=config.EventBusConfig(routing_prefix="prefix", unique=True),
    )
    child = config.RuleWorkflowConfig(name="child", type=WorkflowStepType.regex_parser)
    wf = config.RuleWorkflowConfig(
        name="wf", type=WorkflowStepType.log_query, bucket_by=["job_id"], attributes=attr, children=[child]
    )
    wf_json = wf.to_json()
    wf2 = config.RuleWorkflowConfig.from_json(wf_json)
    assert wf == wf2
    assert wf.name == "wf"
    assert wf.type == WorkflowStepType.log_query
    assert wf.bucket_by == ["job_id"]
    assert wf.attributes.query == {
        "regex": ["re1", {"TRAINING_LOOP": "re2"}],
    }
    assert wf.attributes.events[0].event_type == "type1"
    assert wf.attributes.events[0].regexes == ["regex1"]
    assert wf.attributes.send_message_eventbus.routing_prefix == "prefix"
    assert wf.attributes.send_message_eventbus.unique is True
    assert wf.children[0].name == "child"
    assert wf.children[0].type == WorkflowStepType.regex_parser


def test_rule_config_and_serialization():
    wf = config.RuleWorkflowConfig(name="wf", type=WorkflowStepType.nooutput)
    rule = config.RuleConfig(name="r1", workflow=[wf])
    rule_json = rule.to_json()
    rule2 = config.RuleConfig.from_json(rule_json)
    assert rule == rule2
    assert rule.name == "r1"
    assert rule.workflow[0].name == "wf"
    assert rule.workflow[0].type == WorkflowStepType.nooutput


def test_watcher_config_full_and_serialization():
    policy = config.LogQueryPolicyConfig()
    ds = config.DatasourcesConfig(
        log_query_policy=policy,
        log_query=config.LogQueryConfig.from_json({"providers": {"loki": {"url": "http://localhost"}}}),
    )
    wf = config.RuleWorkflowConfig(name="wf", type=WorkflowStepType.emit_metric)
    rule = config.RuleConfig(name="r1", workflow=[wf])
    watcher = config.WatcherConfig(
        metrics_cleanup_threshold_secs=123,
        datasources=ds,
        event_source=config.WatcherEventSource.LOG_QUERY,
        rules=[rule],
    )
    watcher_json = watcher.to_json()
    watcher2 = config.WatcherConfig.from_json(watcher_json)
    assert watcher == watcher2
    assert watcher.metrics_cleanup_threshold_secs == 123
    assert watcher.datasources.log_query.selected_provider.url == "http://localhost"
    assert watcher.event_source == config.WatcherEventSource.LOG_QUERY
    assert watcher.rules[0].name == "r1"
    assert watcher.rules[0].workflow[0].type == WorkflowStepType.emit_metric


def test_helm_default_workflows_use_supported_step_types():
    repository_root = Path(__file__).resolve().parents[3]
    values_path = repository_root / "deploy" / "helm" / "watcher" / "values.yaml"
    values = yaml.safe_load(values_path.read_text())

    workflow_config = values["config"]
    workflows = workflow_config["anomaly_workflow_definitions"]
    workflows.extend(workflow_config.get("application_workflows", []))

    parsed_workflows = [config.RuleWorkflowConfig.from_json(workflow) for workflow in workflows]

    assert parsed_workflows


def make_test_config_for_regex_and_search():
    # Helper to create a WatcherConfig with nested workflows for testing
    attr1 = config.RuleAttributesConfig(query={"regex": ["re1", "re2"]})
    attr2 = config.RuleAttributesConfig(query={"regex": ["re3"]})
    attr_child = config.RuleAttributesConfig(query={"regex": ["re4"]})
    child = config.RuleWorkflowConfig(name="child", type=WorkflowStepType.regex_parser, attributes=attr_child)
    wf1 = config.RuleWorkflowConfig(name="wf1", type=WorkflowStepType.log_query, attributes=attr1, children=[child])
    wf2 = config.RuleWorkflowConfig(name="wf2", type=WorkflowStepType.log_query, attributes=attr2)
    rule = config.RuleConfig(name="r1", workflow=[wf1, wf2])
    watcher = config.WatcherConfig(
        datasources=config.DatasourcesConfig(
            log_query=config.LogQueryConfig.from_json(
                {
                    "providers": {
                        "loki": {
                            "url": "http://localhost",
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


def test_extract_regex_queries():
    watcher, *_ = make_test_config_for_regex_and_search()
    result = watcher.extract_regex_queries
    # Should include wf1, wf2, and child
    assert len(result) == 3
    names = {d["name"] for d in result}
    assert names == {"wf1", "wf2", "child"}
    for d in result:
        assert isinstance(d["regex"], list)
        assert all(isinstance(x, str) for x in d["regex"])
    # Check regex values
    assert any(d["regex"] == ["re1", "re2"] for d in result)
    assert any(d["regex"] == ["re3"] for d in result)
    assert any(d["regex"] == ["re4"] for d in result)


def test_find_workflow_by_name_type():
    watcher, wf1, wf2, child = make_test_config_for_regex_and_search()
    # Root node
    found = watcher.find_workflow_by_name_type("wf1", WorkflowStepType.log_query)
    assert found is wf1
    # Second root node
    found = watcher.find_workflow_by_name_type("wf2", WorkflowStepType.log_query)
    assert found is wf2
    # Child node (recursive)
    found = watcher.find_workflow_by_name_type("child", WorkflowStepType.regex_parser)
    assert found is child
    # Not found
    found = watcher.find_workflow_by_name_type("notfound", WorkflowStepType.log_query)
    assert found is None


@pytest.mark.parametrize("grace", [0, -1, 1.5, True, "30"])
def test_nooutput_discovery_grace_rejects_invalid_values(grace):
    """Reject delays that would bypass the grace period or break timer arithmetic."""
    with pytest.raises(ValueError):
        config.RuleAttributesConfig.from_json({"job_discovery_grace_period_secs": grace})


def test_nooutput_discovery_grace_round_trip():
    """Keep the operator's retry delay when loading and serializing rule attributes."""
    attrs = config.RuleAttributesConfig.from_json({"job_discovery_grace_period_secs": 60})
    assert attrs.to_json()["job_discovery_grace_period_secs"] == 60
