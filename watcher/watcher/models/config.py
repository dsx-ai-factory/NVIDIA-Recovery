# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from dataclasses import MISSING, asdict, dataclass, field, is_dataclass
from enum import Enum
from functools import cached_property
from typing import Any, Optional

from pydantic import BaseModel
from recovery_libs.audit import AuditTrailSettings
from recovery_libs.event_bus.event_bus_config import EventBusConfig as ListenerEventBusConfig
from recovery_libs.event_bus.event_bus_config import EventBusPayload
from recovery_libs.log_query import BackendName

from watcher.lib.utils.constants import DEFAULT_QUEUE_TIMEOUT_SECS
from watcher.models.workflow import WorkflowStepType


def _enum_from_value(enum_cls, value):
    """
    Convert a value to an enum instance, handling various input types.

    Args:
        enum_cls (Type[Enum]): The enum class to convert to
        value (Any): The value to convert, can be None, an enum instance, or a valid enum value

    Returns:
        Enum | None: The enum instance or None if the input was None

    Example:
        >>> class Color(Enum):
        ...     RED = "red"
        ...     BLUE = "blue"
        >>> _enum_from_value(Color, "red")
        <Color.RED: 'red'>
        >>> _enum_from_value(Color, Color.RED)
        <Color.RED: 'red'>
        >>> _enum_from_value(Color, None)
        None
    """
    if value is None:
        return None
    if isinstance(value, enum_cls):
        return value
    return enum_cls(value)


def _sanitize_json(obj: Any) -> Any:
    """
    Recursively remove None values from dictionaries and lists.

    Args:
        obj (Any): The object to sanitize, can be a dict, list, Enum, or any other type

    Returns:
        Any: The sanitized object with None values removed

    Example:
        >>> data = {
        ...     "a": None,
        ...     "b": [1, None, 3],
        ...     "c": {"d": None, "e": "value"}
        ... }
        >>> _sanitize_json(data)
        {'b': [1, 3], 'c': {'e': 'value'}}
    """
    if obj is None:
        return None

    if isinstance(obj, BaseModel):
        return _sanitize_json(obj.model_dump(mode="json"))

    if isinstance(obj, dict):
        return {k: _sanitize_json(v) for k, v in obj.items() if v is not None}

    if isinstance(obj, list):
        return [_sanitize_json(item) for item in obj if item is not None]

    if isinstance(obj, Enum):
        return obj.value

    return obj


def _to_json(obj: Any) -> Any:
    """
    Convert an object to a dictionary, handling nested dataclasses and complex types.

    Args:
        obj (Any): The object to convert, can be a dataclass, dict, list, Enum, or any other type

    Returns:
        Dict[str, Any]: A dictionary representation of the object
    """
    if obj is None:
        return {}

    if is_dataclass(obj):
        result = {}
        for field_name, field_value in asdict(obj).items():  # type: ignore[arg-type]
            if field_value is not None:
                if is_dataclass(field_value):
                    result[field_name] = _to_json(field_value)
                elif isinstance(field_value, list):
                    result[field_name] = [_to_json(item) if is_dataclass(item) else item for item in field_value]
                elif isinstance(field_value, dict):
                    result[field_name] = {k: _to_json(v) if is_dataclass(v) else v for k, v in field_value.items()}
                else:
                    result[field_name] = field_value

        return _sanitize_json(result)

    if isinstance(obj, dict):
        return _sanitize_json({k: _to_json(v) for k, v in obj.items()})

    if isinstance(obj, list):
        return _sanitize_json([_to_json(item) for item in obj])

    if hasattr(obj, "to_json"):
        return _sanitize_json(_to_json(obj.to_json()))

    if isinstance(obj, Enum):
        return obj.value

    return obj


def _from_json[T](cls: type[T], data: dict | None) -> T:
    """
    Convert a dictionary to an instance of the specified class.

    Args:
        cls (Type[T]): The target class to convert to
        data (dict): The dictionary data to convert

    Returns:
        T: An instance of the specified class

    Example:
        >>> @dataclass
        ... class Address:
        ...     street: str
        ...     city: str
        >>> @dataclass
        ... class Person:
        ...     name: str
        ...     address: Address
        >>> data = {
        ...     "name": "John",
        ...     "address": {"street": "123 Main St", "city": "Boston"}
        ... }
        >>> _from_json(Person, data)
        Person(name='John', address=Address(street='123 Main St', city='Boston'))
    """
    if data is None:
        return None  # type: ignore[return-value]
    if isinstance(data, cls):  # Already deserialized
        return data
    if not hasattr(cls, "__dataclass_fields__"):
        return data  # type: ignore[return-value]
    kwargs = {}
    for field_name, field_def in cls.__dataclass_fields__.items():  # type: ignore[attr-defined]
        value = data.get(field_name)
        typ = field_def.type
        if value is not None:
            # Handle enums
            if typ is WatcherEventSource:
                kwargs[field_name] = _enum_from_value(WatcherEventSource, value)
            elif typ is WorkflowStepType:
                kwargs[field_name] = _enum_from_value(WorkflowStepType, value)
            # Handle nested Pydantic models
            elif isinstance(typ, type) and issubclass(typ, BaseModel):
                kwargs[field_name] = typ.model_validate(value)
            # Handle nested dataclasses
            elif hasattr(typ, "__dataclass_fields__"):
                kwargs[field_name] = _from_json(typ, value)
            # Handle lists of dataclasses
            elif getattr(typ, "__origin__", None) is list:
                elem_type = typ.__args__[0]
                kwargs[field_name] = [_from_json(elem_type, v) if isinstance(v, dict) else v for v in value]
            # Handle dicts
            elif getattr(typ, "__origin__", None) is dict:
                kwargs[field_name] = value
            else:
                kwargs[field_name] = value
        else:
            # Use the default_factory if available and callable, otherwise use the default value
            if hasattr(field_def, "default_factory") and callable(field_def.default_factory):
                kwargs[field_name] = field_def.default_factory()
            elif hasattr(field_def, "default"):
                kwargs[field_name] = field_def.default
            else:
                kwargs[field_name] = None

    # Create instance and run post-init validation
    instance = cls(**kwargs)
    if hasattr(instance, "__post_init__"):
        instance.__post_init__()

    return instance


class WatcherEventSource(str, Enum):
    LOG_QUERY = "log_query"
    LOKI = "loki"
    RABBITMQ = "rabbitmq"

    @property
    def uses_log_query(self) -> bool:
        """Return whether this source uses the configured query backend."""

        return self in (WatcherEventSource.LOG_QUERY, WatcherEventSource.LOKI)


class RuleAttributesQueryType(str, Enum):
    """Workflow-local matching is provider-neutral; native queries live in providers."""

    REGEX = "regex"


@dataclass
class BaseConfig:
    """
    Base class for all configuration dataclasses that provides post-init validation.
    """

    def __post_init__(self):
        """
        Validate that non-optional attributes are not set to _MISSING_TYPE.
        Raises ValueError if any required fields are missing.
        """
        for field_name, field_value in self.__dict__.items():
            if isinstance(field_value, type(MISSING)):
                raise ValueError(f"Required field '{field_name}' is missing in {self.__class__.__name__}")  # noqa: TRY004

    def to_json(self):
        return _to_json(self)

    @classmethod
    def from_json(cls, data):
        if data is None or isinstance(data, cls):
            return data
        return _from_json(cls, data)


@dataclass
class LabelRemappingCompositeConfig(BaseConfig):
    """
    Configuration for composite label remapping.

    Combines multiple backend fields into a single Event attribute using a template string.

    Attributes:
        attribute: Target attribute name in Event.Attributes
        template: Format string with {label_name} placeholders
        labels: List of backend field names referenced in the template
    """

    attribute: str
    template: str
    labels: list[str] = field(default_factory=list)


@dataclass
class LabelRemappingConfig(BaseConfig):
    """
    Configuration for remapping backend fields to Event.Attributes.

    Supports two types of remapping:
    - simple: Direct 1:1 mapping from backend field to attribute name
    - composite: Combine multiple fields using a template string

    Example:
        simple:
          job_id: slurm_job_id  # Maps provider field 'slurm_job_id' to 'job_id'
        composite:
          - attribute: log_file_path
            template: "/var/log/{job_name}/{job_id}"
            labels: [job_name, job_id]
    """

    simple: dict[str, str] | None = field(default_factory=dict)
    composite: list[LabelRemappingCompositeConfig] | None = field(default_factory=list)

    @classmethod
    def from_json(cls, data):
        if data is None or isinstance(data, cls):
            return data
        data = dict(data)
        if "composite" in data and data["composite"] is not None:
            data["composite"] = [
                LabelRemappingCompositeConfig.from_json(c) if isinstance(c, dict) else c for c in data["composite"]
            ]
        return _from_json(cls, data)


@dataclass
class LogQueryPolicyConfig(BaseConfig):
    """Watcher polling and record-normalization policy.

    New configuration uses ``datasources.log_query_policy``. The legacy
    ``datasources.loki`` key still loads this model during the compatibility
    window, but none of these controls select a provider.
    """

    # Kept only to provide an actionable migration error for old values.
    url: str = ""
    query_delay: int | None = None
    query_limit: int | None = None
    query_max_lookback: int | None = None
    query_timeout: int | None = None
    ingestion_delay_timestamp_regex: str | None = None
    ingestion_delay_timestamp_timezone: str | None = None
    ingestion_delay_metric_collection_interval_secs: int | None = None
    start_override: str | None = None
    end_override: str | None = None
    label_remapping_enabled: bool | None = False
    label_remapping: LabelRemappingConfig | None = None

    def __post_init__(self):
        super().__post_init__()
        if self.url:
            raise ValueError(
                "Move log-query connection URL from polling policy to datasources.log_query.providers.<backend>.url"
            )
        if self.query_delay is not None and (
            isinstance(self.query_delay, bool) or not isinstance(self.query_delay, int) or self.query_delay < 0
        ):
            raise ValueError("LogQueryPolicyConfig.query_delay must be a non-negative integer number of seconds")
        if self.query_max_lookback is not None and (
            isinstance(self.query_max_lookback, bool)
            or not isinstance(self.query_max_lookback, int)
            or self.query_max_lookback <= 0
        ):
            raise ValueError("LogQueryPolicyConfig.query_max_lookback must be a positive integer number of seconds")

    @classmethod
    def from_json(cls, data):
        if data is None or isinstance(data, cls):
            return data
        data = dict(data)
        if "query_prefix" in data:
            raise ValueError("Move query_prefix to datasources.log_query.providers.<backend>.queries.query_prefix")
        for key in ("tenants", "org_id", "x_scope_org_id", "account_id", "project_id"):
            if data.get(key) not in (None, ""):
                raise ValueError(
                    f"Move log-query {key} from polling policy to datasources.log_query.providers.<backend>"
                )
        if "label_remapping" in data and data["label_remapping"] is not None:
            data["label_remapping"] = LabelRemappingConfig.from_json(data["label_remapping"])
        return _from_json(cls, data)


@dataclass
class WorkflowNativeQueries(BaseConfig):
    """Ordered native row queries and optional no-output validation for one workflow."""

    query: list[str | dict[str, str]]
    nooutput_validation_query: str | None = None

    @classmethod
    def from_json(cls, data):
        """Load a native workflow without treating phase maps as model objects."""

        if isinstance(data, cls):
            return data
        if not isinstance(data, dict) or "query" not in data:
            raise ValueError("Provider workflow must be a mapping with a query list")
        if set(data) - {"query", "nooutput_validation_query"}:
            raise ValueError("Provider workflow accepts only query and nooutput_validation_query")
        return cls(**data)

    def __post_init__(self):
        super().__post_init__()
        if not isinstance(self.query, list) or not self.query:
            raise ValueError("Provider workflow query must be a nonempty list")
        for item in self.query:
            if isinstance(item, str):
                valid = bool(item.strip())
            elif isinstance(item, dict):
                valid = bool(item) and all(
                    isinstance(key, str) and key.strip() and isinstance(value, str) and value.strip()
                    for key, value in item.items()
                )
            else:
                valid = False
            if not valid:
                raise ValueError("Provider workflow query must contain nonblank strings or phase maps")
        if self.nooutput_validation_query is not None and (
            not isinstance(self.nooutput_validation_query, str) or not self.nooutput_validation_query.strip()
        ):
            raise ValueError("Provider nooutput validation query must not be blank")


@dataclass
class ProviderNativeQueries(BaseConfig):
    """Native query prefix and templates keyed by existing workflow names."""

    query_prefix: str = ""
    workflows: dict[str, WorkflowNativeQueries] = field(default_factory=dict)

    def __post_init__(self):
        super().__post_init__()
        if not isinstance(self.query_prefix, str):
            raise ValueError("Provider queries.query_prefix must be a string")  # noqa: TRY004
        if not isinstance(self.workflows, dict):
            raise ValueError("Provider queries.workflows must be a mapping")  # noqa: TRY004

    @classmethod
    def from_json(cls, data):
        """Deserialize the provider-owned workflow templates."""

        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError("Provider queries must be a mapping")  # noqa: TRY004
        if set(data) - {"query_prefix", "workflows"}:
            raise ValueError("Provider queries accepts only query_prefix and workflows")
        data = dict(data)
        workflows = data.get("workflows", {})
        if not isinstance(workflows, dict):
            raise ValueError("Provider queries.workflows must be a mapping")  # noqa: TRY004
        if any(not isinstance(name, str) or not name.strip() for name in workflows):
            raise ValueError("Provider queries.workflows names must be nonblank strings")
        data["workflows"] = {name: WorkflowNativeQueries.from_json(value) for name, value in workflows.items()}
        return _from_json(cls, data)


@dataclass
class LogQueryProviderConfig(BaseConfig):
    """Connection and native-query settings for one log-query provider."""

    url: str
    queries: ProviderNativeQueries = field(default_factory=ProviderNativeQueries)
    tenants: str | None = None
    org_id: str | None = None
    account_id: int | None = None
    project_id: int | None = None
    require_tenant: bool = False

    @classmethod
    def from_json(cls, data):
        """Load provider settings, accepting Helm's decimal-string tenant IDs."""

        if data is None or isinstance(data, cls):
            return data
        data = dict(data)
        if "query_prefix" in data:
            raise ValueError(
                "Move provider query_prefix to datasources.log_query.providers.<backend>.queries.query_prefix"
            )
        if "queries" in data:
            data["queries"] = ProviderNativeQueries.from_json(data["queries"])
        for key in ("account_id", "project_id"):
            value = data.get(key)
            if isinstance(value, str) and value and all("0" <= character <= "9" for character in value):
                data[key] = int(value, 10)
        return _from_json(cls, data)

    def __post_init__(self):
        super().__post_init__()
        if not self.url:
            raise ValueError("log-query provider url must not be empty")
        if (self.account_id is None) != (self.project_id is None):
            raise ValueError("log-query provider account_id and project_id must be configured together")
        for value in (self.account_id, self.project_id):
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > 4_294_967_295
            ):
                raise ValueError("log-query provider account_id and project_id must be uint32 values")
        if self.require_tenant and self.account_id is None:
            raise ValueError("log-query provider account_id and project_id are required when require_tenant is true")


@dataclass
class LogQueryProvidersConfig(BaseConfig):
    """Statically known provider configurations."""

    loki: LogQueryProviderConfig | None = None
    victorialogs: LogQueryProviderConfig | None = None

    @classmethod
    def from_json(cls, data):
        if data is None or isinstance(data, cls):
            return data
        data = dict(data)
        for backend in BackendName:
            provider = data.get(backend.value)
            if isinstance(provider, dict) and "query_prefix" in provider:
                raise ValueError(
                    "Move provider query_prefix to datasources.log_query.providers.<backend>.queries.query_prefix"
                )
            if isinstance(provider, dict) and not provider.get("url"):
                data[backend.value] = None
            elif provider is not None and not isinstance(provider, LogQueryProviderConfig):
                data[backend.value] = LogQueryProviderConfig.from_json(provider)
        return _from_json(cls, data)


@dataclass
class LogQueryConfig(BaseConfig):
    """Select the active log-query backend and its provider configuration."""

    backend: BackendName = BackendName.LOKI
    providers: LogQueryProvidersConfig = field(default_factory=LogQueryProvidersConfig)

    def __post_init__(self):
        super().__post_init__()
        self.backend = _enum_from_value(BackendName, self.backend)
        if self.selected_provider is None:
            raise ValueError(
                f"datasources.log_query.providers.{self.backend.value} is required when backend is {self.backend.value}"
            )

    @property
    def selected_provider(self) -> LogQueryProviderConfig | None:
        """Return the provider selected by ``backend``."""

        return getattr(self.providers, self.backend.value)

    @classmethod
    def from_json(cls, data):
        if data is None or isinstance(data, cls):
            return data
        data = dict(data)
        if "backend" in data and data["backend"] is not None:
            data["backend"] = _enum_from_value(BackendName, data["backend"])
        if "providers" in data and data["providers"] is not None:
            data["providers"] = LogQueryProvidersConfig.from_json(data["providers"])
        return _from_json(cls, data)


@dataclass
class RabbitMQPublisherConfig(BaseConfig):
    host: str
    username: str
    password: str = field(repr=False)
    vhost: str
    exchange: str
    routing_prefix: str = "anomaly.detected."
    queues: list[str] | None = field(default_factory=list)
    routing_keys: dict[str, list[str]] | None = field(default_factory=dict)

    def __post_init__(self):
        if env_password := os.environ.get("RABBITMQ_PASSWORD"):
            self.password = env_password

        if not self.username or not self.password:
            raise ValueError("Missing RMQ credentials")


@dataclass
class RabbitMQSubscriberConfig(BaseConfig):
    """Watcher input settings, retaining the legacy protobuf boolean.

    ``queue_max_size_mb`` limits broker queue bytes in MiB; zero is unlimited.
    """

    host: str
    username: str
    password: str = field(repr=False)
    vhost: str
    exchange_name: str
    queue_name: str
    routing_key: str
    is_payload_protobuf: bool = False
    exchange_type: str | None = None
    queue_full_timeout_sec: int | None = None
    queue_max_size_mb: int = 0

    def __post_init__(self):
        if env_password := os.environ.get("RABBITMQ_PASSWORD"):
            self.password = env_password

        if not self.username or not self.password:
            raise ValueError("Missing RMQ credentials")

    def to_event_bus_config(self) -> ListenerEventBusConfig:
        """Translate Watcher settings into the shared listener's contract.

        Omitted exchange and timeout settings use a direct exchange and the
        Watcher queue timeout. Preserve explicit zero timeouts for nonblocking
        delivery to the local queue.
        """
        return ListenerEventBusConfig(
            host=self.host,
            username=self.username,
            password=self.password,
            vhost=self.vhost,
            exchange_name=self.exchange_name,
            exchange_type=self.exchange_type if self.exchange_type is not None else "direct",
            queue_name=self.queue_name,
            routing_key=self.routing_key,
            queue_full_timeout_sec=(
                self.queue_full_timeout_sec if self.queue_full_timeout_sec is not None else DEFAULT_QUEUE_TIMEOUT_SECS
            ),
            queue_max_size_mb=self.queue_max_size_mb,
            payload=EventBusPayload.PROTOBUF_LOGS if self.is_payload_protobuf else EventBusPayload.JSON,
        )


@dataclass
class RabbitMQConfig(BaseConfig):
    publisher: RabbitMQPublisherConfig
    subscriber: RabbitMQSubscriberConfig | None = None

    @classmethod
    def from_json(cls, data):
        if data is None or isinstance(data, cls):
            return data
        data = dict(data)
        if (
            "publisher" in data
            and data["publisher"] is not None
            and not isinstance(data["publisher"], RabbitMQPublisherConfig)
        ):
            data["publisher"] = RabbitMQPublisherConfig.from_json(data["publisher"])
        if (
            "subscriber" in data
            and data["subscriber"] is not None
            and not isinstance(data["subscriber"], RabbitMQSubscriberConfig)
        ):
            data["subscriber"] = RabbitMQSubscriberConfig.from_json(data["subscriber"])
        return _from_json(cls, data)


@dataclass
class JobManagerConfig(BaseConfig):
    endpoint: str | None = None


@dataclass
class DatasourcesConfig(BaseConfig):
    log_query_policy: LogQueryPolicyConfig | None = None
    # Legacy name for ``log_query_policy``. It remains input-only compatibility
    # state and never selects the backend.
    loki: LogQueryPolicyConfig | None = None
    log_query: LogQueryConfig | None = None
    rabbitmq: RabbitMQConfig | None = None
    job_manager: JobManagerConfig | None = None

    @property
    def log_query_backend(self) -> BackendName:
        """Return the selected query backend, defaulting legacy configurations to Loki."""

        return self.log_query.backend if self.log_query is not None else BackendName.LOKI

    @property
    def selected_log_query_policy(self) -> LogQueryPolicyConfig | None:
        """Return neutral polling policy with legacy Loki fallback."""

        return self.log_query_policy or self.loki

    @property
    def log_query_prefix(self) -> str:
        """Return the active provider-native query prefix."""

        if self.log_query is not None:
            provider = self.log_query.selected_provider
            if provider is not None:
                return provider.queries.query_prefix
        return ""

    def workflow_queries(self, name: str) -> WorkflowNativeQueries:
        """Return the selected provider's native templates for a logical workflow."""

        provider = self.log_query.selected_provider if self.log_query is not None else None
        if provider is None or name not in provider.queries.workflows:
            raise ValueError(
                f"Workflow '{name}' requires datasources.log_query.providers.{self.log_query_backend.value}"
                f".queries.workflows.{name}"
            )
        return provider.queries.workflows[name]

    @classmethod
    def from_json(cls, data):
        if data is None or isinstance(data, cls):
            return data
        data = dict(data)
        if (
            "log_query_policy" in data
            and data["log_query_policy"] is not None
            and not isinstance(data["log_query_policy"], LogQueryPolicyConfig)
        ):
            data["log_query_policy"] = LogQueryPolicyConfig.from_json(data["log_query_policy"])
        if "loki" in data and data["loki"] is not None and not isinstance(data["loki"], LogQueryPolicyConfig):
            data["loki"] = LogQueryPolicyConfig.from_json(data["loki"])
        if "log_query" in data and data["log_query"] is not None and not isinstance(data["log_query"], LogQueryConfig):
            data["log_query"] = LogQueryConfig.from_json(data["log_query"])
        if "rabbitmq" in data and data["rabbitmq"] is not None and not isinstance(data["rabbitmq"], RabbitMQConfig):
            data["rabbitmq"] = RabbitMQConfig.from_json(data["rabbitmq"])
        if (
            "job_manager" in data
            and data["job_manager"] is not None
            and not isinstance(data["job_manager"], JobManagerConfig)
        ):
            data["job_manager"] = JobManagerConfig.from_json(data["job_manager"])
        return _from_json(cls, data)


@dataclass
class RuleActionConfig(BaseConfig):
    send_message_eventbus: str | None = None
    unique: bool | None = None


@dataclass
class EventRegex(BaseConfig):
    event_type: str
    regexes: list[str]


@dataclass
class EventBusConfig(BaseConfig):
    routing_prefix: str
    unique: bool | None = None


@dataclass
class RuleAttributesConfig(BaseConfig):
    query: dict[RuleAttributesQueryType, list[str | dict[str, str]]] | None = None
    regex: str | None = None
    cache_key: str | None = None
    action: Optional["RuleActionConfig"] = None
    metric_name: str | None = None
    metric_type: str | None = None
    enable_metric_cleanup: bool | None = None
    interval: Any | None = None
    # No-output lookup retry delay in seconds. When omitted, the default of 31 seconds is used.
    job_discovery_grace_period_secs: int | None = None
    last_iteration_enabled: bool | None = None
    last_iteration_regex: str | None = None
    last_iteration_operation_schema: str | None = None
    false_positive_scan_cache_key: str | None = None
    false_positive_scan_timer: int | None = None
    log_timezone: str | None = None
    events: list[EventRegex] | None = None
    send_message_eventbus: EventBusConfig | None = None
    # REGEX_PARSER workflow step
    advanced_regex: list[dict[str, Any]] | None = None
    dedup: bool | None = None
    dedup_signature_pattern: str | None = None
    dedup_strip_patterns: list[str] | None = None
    event_type: str | None = None
    publish_events: bool | None = None
    stop_after_first_match: bool | None = None
    # Log-query workflow step
    filter_events_by_jobid_before_processing: bool | None = None
    # STRAGGLER workflow step
    # See watcher/docs/ANOMALY_DETECTION.md for more information
    window_size: int | None = None
    base_percentile_threshold: int | None = None
    anomaly_percent_threshold: int | None = None
    alert_smoothing_count: int | None = None
    warmup_grace_period_secs: int | None = None
    min_window_size_for_detection: int | None = None
    min_interval_between_datapoints_secs: int | None = None
    skip_too_close_datapoints: bool | None = None
    reset_after_good_values: int | None = None
    notify_once: bool | None = None
    ## STRAGGLER workflow step - to be removed once all environments are updated
    operation: str | None = None
    anomaly_threshold: int | None = None

    def __post_init__(self):
        super().__post_init__()
        grace = self.job_discovery_grace_period_secs
        if grace is not None and (type(grace) is not int or grace <= 0):
            raise ValueError("job_discovery_grace_period_secs must be a positive integer")
        if self.query is not None and any(key != RuleAttributesQueryType.REGEX for key in self.query):
            raise ValueError(
                "Move native workflow queries to datasources.log_query.providers.<backend>.queries.workflows.<name>"
            )

    @classmethod
    def from_json(cls, data):
        if data is None or isinstance(data, cls):
            return data
        data = dict(data)
        if "nooutput_validation_query" in data:
            raise ValueError("Move nooutput_validation_query to the provider queries.workflows.<name> block")
        # Validate query structure
        if "query" in data and data["query"] is not None:
            q = data["query"]
            if not isinstance(q, dict):
                raise ValueError("RuleAttributesConfig.query must be a dict")
            if any(key != RuleAttributesQueryType.REGEX for key in q):
                raise ValueError(
                    "Move native workflow queries to datasources.log_query.providers.<backend>.queries.workflows.<name>"
                )
            # Convert string keys to QueryKey enum
            data["query"] = {RuleAttributesQueryType(k): v for k, v in q.items()}
            for k, v in data["query"].items():
                if not isinstance(v, list):
                    raise ValueError(f"RuleAttributesConfig.query['{k}'] must be a list")  # noqa: TRY004
                for item in v:
                    if not (
                        isinstance(item, str)
                        or (
                            isinstance(item, dict)
                            and all(isinstance(kk, str) and isinstance(vv, str) for kk, vv in item.items())
                        )
                    ):
                        raise ValueError(f"RuleAttributesConfig.query['{k}'] items must be str or dict[str, str]")

        # Handle events
        if "events" in data and data["events"] is not None:
            data["events"] = [EventRegex(**event) for event in data["events"]]

        # Handle send_message_eventbus
        if "send_message_eventbus" in data and data["send_message_eventbus"] is not None:
            data["send_message_eventbus"] = EventBusConfig(**data["send_message_eventbus"])

        return _from_json(cls, data)


@dataclass
class RuleWorkflowConfig(BaseConfig):
    name: str
    type: WorkflowStepType
    bucket_by: list[str] = field(default_factory=list)
    attributes: RuleAttributesConfig | None = None
    children: list["RuleWorkflowConfig"] = field(default_factory=list)

    @classmethod
    def from_json(cls, data):
        if data is None or isinstance(data, cls):
            return data
        data = dict(data)
        if "children" in data and data["children"] is not None:
            data["children"] = [RuleWorkflowConfig.from_json(child) for child in data["children"]]
        if "attributes" in data and data["attributes"] is not None:
            data["attributes"] = RuleAttributesConfig.from_json(data["attributes"])
        if "type" in data and data["type"] is not None:
            data["type"] = _enum_from_value(WorkflowStepType, data["type"])

        return _from_json(cls, data)


@dataclass
class RuleConfig(BaseConfig):
    name: str
    workflow: list[RuleWorkflowConfig]

    @classmethod
    def from_json(cls, data):
        if data is None or isinstance(data, cls):
            return data
        data = dict(data)
        if "workflow" in data and data["workflow"] is not None:
            data["workflow"] = [RuleWorkflowConfig.from_json(w) for w in data["workflow"]]

        return _from_json(cls, data)


@dataclass
class WatcherServiceConfig(BaseConfig):
    host: str = "0.0.0.0"
    port: int = 8080
    http_log_level: str = "WARNING"
    max_queue_size: int = 1024
    maintenance_tasks_interval_secs: int = 30
    num_threads: int = 5
    notification_dedup_ttl_secs: int = 300
    audit_trail: AuditTrailSettings = field(default_factory=AuditTrailSettings)


@dataclass
class WatcherConfig(BaseConfig):
    datasources: DatasourcesConfig
    rules: list[RuleConfig] = field(default_factory=list)
    rules_path: str | None = None
    service: "WatcherServiceConfig" = field(default_factory=lambda: WatcherServiceConfig())
    metrics_cleanup_threshold_secs: int = 300
    event_source: WatcherEventSource = field(default=WatcherEventSource.LOG_QUERY)

    def __post_init__(self):
        super().__post_init__()
        if not self.event_source.uses_log_query:
            return

        if self.datasources.log_query is None:
            raise ValueError(
                "datasources.log_query with an explicit provider connection is required for log-query polling"
            )

        def _validate_workflow(workflow: RuleWorkflowConfig) -> None:
            requires_native_query = workflow.type in (
                WorkflowStepType.log_query,
                WorkflowStepType.loki_query,
                WorkflowStepType.nooutput,
            )
            if requires_native_query:
                native = self.datasources.workflow_queries(workflow.name)
                for item in native.query:
                    if isinstance(item, dict):
                        if workflow.type != WorkflowStepType.nooutput:
                            raise ValueError(f"Workflow '{workflow.name}' only accepts string row queries")
                        if any(phase not in ("PRE_TRAINING", "TRAINING_LOOP") for phase in item):
                            raise ValueError(f"Workflow '{workflow.name}' has an unknown nooutput query phase")
                if workflow.type == WorkflowStepType.nooutput:
                    template = native.nooutput_validation_query
                    if template is None:
                        raise ValueError(f"Workflow '{workflow.name}' is missing provider nooutput validation query")
                    if "{job_id}" not in template:
                        raise ValueError(
                            f"Workflow '{workflow.name}' nooutput validation query "
                            "must contain the literal placeholder {job_id}"
                        )
            for child in workflow.children:
                _validate_workflow(child)

        for rule in self.rules:
            for workflow in rule.workflow:
                _validate_workflow(workflow)

    @classmethod
    def from_json(cls, data):
        if data is None or isinstance(data, cls):
            return data
        data = dict(data)
        if "datasources" in data and data["datasources"] is not None:
            data["datasources"] = DatasourcesConfig.from_json(data["datasources"])
        if "event_source" in data and data["event_source"] is not None:
            data["event_source"] = _enum_from_value(WatcherEventSource, data["event_source"])
        if "rules" in data and data["rules"] is not None:
            data["rules"] = [RuleConfig.from_json(r) for r in data["rules"]]
        if "service" in data and data["service"] is not None:
            data["service"] = WatcherServiceConfig.from_json(data["service"])
        else:
            data["service"] = WatcherServiceConfig()

        return _from_json(cls, data)

    @cached_property
    def extract_regex_queries(self):
        """
        Extract all regex queries from the configuration's rules and workflows.

        This method traverses the entire rule configuration tree to find all regex patterns
        defined in workflow steps. It's particularly useful for:
        1. Extracting all regex patterns from the configuration for workflow routing.
        2. Validating regex patterns before deployment

        The method handles both simple regex patterns and job phase-specific patterns.
        For example, it will extract patterns from configurations like:
        ```yaml
        query:
          regex:
            - 'pattern1'                    # Simple pattern
            - {'TRAINING_LOOP': 'pattern2'} # Phase-specific pattern
        ```

        Returns:
            List[dict]: A list of dictionaries, each containing:
                - name: The workflow step name
                - type: The workflow step type
                - regex: List of regex patterns (both simple and phase-specific)
        """
        result = []

        def _extract_from_workflow(wf):
            if wf.attributes and wf.attributes.query and "regex" in wf.attributes.query:
                # Extract regex patterns, handling both strings and dictionaries
                regex_patterns = []
                for pattern in wf.attributes.query["regex"]:
                    if isinstance(pattern, dict):
                        regex_patterns.extend(pattern.values())
                    else:
                        regex_patterns.append(pattern)

                result.append(
                    {
                        "name": wf.name,
                        "type": wf.type,
                        "regex": regex_patterns,
                    }
                )

            if wf.children:
                for child in wf.children:
                    _extract_from_workflow(child)

        for rule in self.rules:
            for wf in rule.workflow:
                _extract_from_workflow(wf)

        return result

    def find_workflow_by_name_type(self, name: str, type_: str):
        """
        Search all RuleWorkflowConfig objects (including recursively in children) for a workflow with the given name and type.

        Args:
            name (str): The name to search for.
            type_ (str): The type to search for (as string or WorkflowStepType).

        Returns:
            RuleWorkflowConfig | None: The first matching RuleWorkflowConfig, or None if not found.
        """

        def _search(wf):
            if wf.name == name and (wf.type == type_ or str(wf.type) == str(type_)):
                return wf
            if wf.children:
                for child in wf.children:
                    found = _search(child)
                    if found:
                        return found
            return None

        for rule in self.rules:
            for wf in rule.workflow:
                found = _search(wf)
                if found:
                    return found

        return None
