# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import ast
import collections
import json
import operator
import os
import re
import threading
from dataclasses import dataclass
from datetime import UTC, date, datetime
from functools import lru_cache
from hashlib import sha256
from typing import Any

import uuid6
import yaml
from cachetools import TTLCache

from watcher.lib.utils.caching import VariableTTLLRUCache
from watcher.lib.utils.constants import (
    EVENT_DEDUP_DEFAULT_MAX_SIZE,
    EVENT_DEDUP_DEFAULT_WINDOW_SECS,
)
from watcher.lib.utils.logging import logger
from watcher.lib.utils.multithreading import threadsafe_generator
from watcher.models.config import WatcherConfig

TIMED_COUNTER_DEFAULT_TIMEOUT = 900
TIMED_CACHE_DEFAULT_TIMEOUT = 60 * 60 * 24
TIMED_CACHE_MAX_SIZE = 2048


@dataclass(eq=False)
class _TimedCounterEntry:
    value: Any


class TimedCounter:
    """
    A thread-safe counter class to track of items within a specified
    timeout period.

    Items are added to the counter using the `add` method.
    After the specified timeout, the oldest item is automatically
    removed from the counter.
    """

    def __init__(self, timeout: int = TIMED_COUNTER_DEFAULT_TIMEOUT):
        """
        Initialize the TimedCounter.

        Args:
            timeout (int): The timeout period in seconds.
                           Default is 900 seconds (15 mins).
        """
        self.lock = threading.Lock()
        self.timeout = timeout
        self.items: collections.deque[_TimedCounterEntry] = collections.deque()

    def add(self, item: Any, timeout: int | None = None) -> None:
        """
        Add an item to the counter.

        Args:
            item (Any): The item to be added to the counter.
            timeout (Optional[int]): Per-add timeout in seconds. Uses the counter default when not provided.
        """
        timer_timeout = timeout if timeout is not None else self.timeout
        with self.lock:
            entries = []
            if isinstance(item, list):
                for entry in item:
                    timed_entry = _TimedCounterEntry(entry)
                    self.items.append(timed_entry)
                    entries.append(timed_entry)
            else:
                timed_entry = _TimedCounterEntry(item)
                self.items.append(timed_entry)
                entries.append(timed_entry)

            # Daemonize the eviction timer so a slow/large timeout cannot keep the
            # interpreter alive at shutdown. The timer only performs best-effort eviction
            # of these entries; losing un-fired timers on exit is safe.
            timer = threading.Timer(timer_timeout, self.expire, args=(entries,))
            timer.daemon = True
            timer.start()

    def __len__(self) -> int:
        """
        Get the current number of items in the counter.

        Returns:
            int: The number of items in the counter.
        """
        with self.lock:
            return len(self.items)

    def expire(self, entries: list[_TimedCounterEntry] | None = None):
        """
        Remove item from the counter.

        This method is called automatically after the specified
        timeout.
        """
        with self.lock:
            if entries is None:
                if len(self.items) > 0:
                    self.items.popleft()
                return

            for entry in entries:
                try:
                    self.items.remove(entry)
                except ValueError:
                    pass

    def __str__(self):
        """
        Get a string representation of the items in the counter.

        Returns:
            str: A string representation of the items in the counter.
        """
        with self.lock:
            return str([item.value for item in self.items])

    @threadsafe_generator
    def __iter__(self):
        """
        Return an iterator over the items in the counter.

        Returns:
            iterator: An iterator over the items in the counter.
        """
        return iter(self.tolist())

    @property
    def oldest(self) -> Any:
        """
        Get the first (oldest) item in the counter.

        Returns:
            Any: The first item in the counter.
        Raises:
            IndexError: If the counter is empty.
        """
        with self.lock:
            if not self.items:
                raise IndexError("TimedCounter is empty")
            return self.items[0].value

    @property
    def newest(self) -> Any:
        """
        Get the last (newest) item in the counter.

        Returns:
            Any: The last item in the counter.
        Raises:
            IndexError: If the counter is empty.
        """
        with self.lock:
            if not self.items:
                raise IndexError("TimedCounter is empty")
            return self.items[-1].value

    def get_item(self, index: int) -> Any:
        """
        Get an item from the counter using its index.

        Args:
            index (int): The index of the item to retrieve.
        Returns:
            Any: The item at the specified index.
        Raises:
            IndexError: If the index is out of range.
        """
        with self.lock:
            return self.items[index].value

    def tolist(self) -> list[Any]:
        """
        Returns a list of all items from the counter.

        Returns:
            List[Any]: List of all items at the specified key.
        Raises:
            KeyError: If the key not in cache.
        """
        with self.lock:
            return [item.value for item in self.items]

    def clear(self) -> None:
        """
        Clears all items from the counter.

        Raises:
            KeyError: If the key not in cache.
        """
        with self.lock:
            while len(self.items) > 0:
                self.items.popleft()


class ActionCache:
    """Thread-safe cache with per-item TTL support and list merging capabilities.

    A cache that supports both default TTL for all items and per-item TTL override.
    Provides special handling for list values by merging them when adding to existing keys.
    Built on VariableTTLLRUCache for optimal performance with lazy purging.

    Args:
        max_size: Maximum number of items the cache can hold.
        timeout: Default time-to-live in seconds for items when no TTL is specified.

    Example:
        >>> cache = ActionCache(max_size=100, timeout=300)
        >>> cache.add('notifications', ['alert1'])  # Uses default 300s TTL
        >>> cache.add('temp_data', 'value', ttl=60)  # Custom 60s TTL
        >>> cache.add('notifications', ['alert2'])  # Merges with existing list
    """

    def __init__(self, max_size: int = TIMED_CACHE_MAX_SIZE, timeout: int = TIMED_CACHE_DEFAULT_TIMEOUT):
        """Initialize the ActionCache.

        Args:
            max_size: Maximum number of items the cache can hold.
            timeout: Default time-to-live in seconds for cached items.
        """
        self.items = VariableTTLLRUCache(maxsize=max_size, default_ttl=timeout)
        self.lock = threading.Lock()

    def add(self, key: str, value: Any, ttl: float | None = None) -> None:
        """Add an item to the cache with optional per-item TTL.

        If the key already exists and both the existing value and new value are lists,
        they will be merged. Otherwise, the value is replaced.

        Args:
            key: The cache key.
            value: The value to store. Lists are merged with existing list values.
            ttl: Time-to-live in seconds for this specific item. If None, uses
                the default TTL specified during cache initialization.
        """
        with self.lock:
            if key in self.items:
                existing_value = self.items[key]
                if isinstance(existing_value, list):
                    if isinstance(value, list):
                        merged_value = existing_value + value
                    else:
                        merged_value = existing_value + [value]

                    # Set the merged value with the specified TTL
                    self.items.set(key, merged_value, ttl=ttl)
                    return

            # Set new value with TTL
            self.items.set(key, value, ttl=ttl)

    def delete(self, key: str) -> None:
        """Delete an item from the cache.

        Args:
            key: The cache key to delete.

        Raises:
            KeyError: If the key is not found in the cache.
        """
        with self.lock:
            if key in self.items:
                del self.items[key]

    def __len__(self) -> int:
        """Get the current number of items in the cache.

        Returns:
            The number of items currently in the cache.
        """
        with self.lock:
            return len(self.items)

    def __str__(self):
        """Get a string representation of the cache contents.

        Returns:
            String representation of the cached items.
        """
        with self.lock:
            return str(self.items)

    @threadsafe_generator
    def __iter__(self):
        """Return an iterator over the cache items.

        Returns:
            Iterator over the cache items.
        """
        return iter(self.items)

    def get(self, key: str) -> Any:
        """Get an item from the cache.

        Args:
            key: The cache key to retrieve.

        Returns:
            The cached value associated with the key.

        Raises:
            KeyError: If the key is not found in the cache or has expired.
        """
        with self.lock:
            return self.items[key]

    def ttl(self, key: str) -> float | None:
        """Get remaining time-to-live for a cache key.

        Args:
            key: Cache key to check TTL for.

        Returns:
            Remaining TTL in seconds, or None if no time-based expiry is set.

        Raises:
            KeyError: If the key is not present in the cache or has expired.

        Example:
            >>> cache.add('temp', 'value', ttl=300)
            >>> cache.ttl('temp')
            299.8
        """
        with self.lock:
            return self.items.ttl(key)


class RunningAverage:
    def __init__(self):
        """
        Initialize the RunningAverage instance with a count and total sum.
        """
        self.count = 0
        self.total_sum = 0.0

    def update_average(self, value: float) -> float:
        """
        Update the average with a new value and return the updated average.

        Args:
            new_value (Union[str, float, int]): The new value to include in the average.

        Returns:
            (float): The updated average of all values received so far.
        """
        if type(value) not in (int, float) and not self.is_number_repl_isdigit(value):  # type: ignore[arg-type]
            self.total_sum / self.count

        self.count += 1
        self.total_sum += float(value)
        return self.total_sum / self.count

    @staticmethod
    def is_number_repl_isdigit(s: str) -> bool:
        """
        Returns True if string is a number.
        """
        return s.replace(".", "", 1).isdigit()


@lru_cache
def load_config() -> dict[str, Any]:
    """
    Load configuration data from a YAML file.

    Returns:
        Dict[str, str]: The loaded configuration data as a dictionary.

    Raises:
        OSError: If the specified config file cannot be opened or read.
        yaml.YAMLError: If there is an error parsing the YAML data
                        in the config file.
    """
    try:
        config = os.environ["CONFIG_PATH"]
        with open(config, "r") as fd:
            config_data = yaml.safe_load(fd)
    except OSError as err:
        logger.error(f"Unable to open config file: {err}")
        raise
    except yaml.YAMLError as err:
        logger.error(f"Unable to load config file: {err}")
        raise

    return config_data


def read_api_token_from_file() -> str | None:
    """
    Read data from a file.

    Returns:
        Optional[str]: Content of the file.

    Raises:
        OSError: If the specified config file cannot be opened or read.
    """
    if api_token_path := os.environ.get("LOKI_API_KEY_PATH"):
        try:
            with open(api_token_path, "r") as fd:
                data = fd.readline()
            api_token = data.strip()
        except OSError as err:
            logger.error(f"Unable to open config file: {err}")
            raise
    elif os.environ.get("LOKI_API_TOKEN"):
        api_token = os.environ["LOKI_API_TOKEN"]
    else:
        api_token = None

    return api_token


def evaluate_expression(expression):
    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError as err:
        logger.debug(f"Unable to parse expression: {err}")
        return None

    if not all(
        isinstance(node, (ast.Expression, ast.UnaryOp, ast.unaryop, ast.BinOp, ast.operator, ast.Constant))
        for node in ast.walk(tree)
    ):
        return None

    return eval(compile(tree, filename="", mode="eval"))


def compare_operator(expression):
    try:
        left, op, right = expression.split(None, 2)
        left_value = evaluate_expression(left)
        right_value = evaluate_expression(right)

        if left_value is None or right_value is None:
            return False

        if op == "<":
            return operator.lt(left_value, right_value)
        elif op == "<=":
            return operator.le(left_value, right_value)
        elif op == "==":
            return operator.eq(left_value, right_value)
        elif op == ">=":
            return operator.ge(left_value, right_value)
        elif op == ">":
            return operator.gt(left_value, right_value)
        else:
            return False

    except (ValueError, TypeError):
        return False


def generate_sha256_signature(data: str) -> str:
    """
    Generate a unique signature for a given string.

    Args:
        data (str): The input string.

    Returns:
        str: SHA-256 hash of the query signature.
    """
    seed = data
    return sha256(seed.encode("utf-8")).hexdigest()


def generate_unique_cache_key(key_suffix: str, length: int = 8) -> str:
    """
    Generate a unique cache key.

    Args:
        key_suffix (str): The input string.
        length (int): The length of hash prefix. Defaults to 8.

    Returns:
        str: Cache key ID.
    """
    if not key_suffix:
        raise ValueError(f"Invalid key_suffix argument: {key_suffix}")

    seed = key_suffix
    return f"{sha256(seed.encode('utf-8')).hexdigest()[:length]}_{seed}"


def generate_uuid7() -> str:
    """
    Generate a unique UUID from a random number, and the current time.

    Returns:
        str: UUID.
    """
    return str(uuid6.uuid7())


def convert_datetime_to_timestamp_ns(dt: date | str | float) -> str:
    """
    Converts datetime or timestamp (secs, ms, μs, ns) to timestamp in nanoseconds

    Args:
        dt [Union[datetime.date, str, float, int]]: datetime.date or timestamp in seconds,
                                                    milliseconds, microseconds or nanoseconds

    Returns:
        str: timestamp in nanoseconds
    """
    if not isinstance(dt, date):
        dt = datetime.fromtimestamp(float(dt))

    return str(int(dt.timestamp() * 1e9))  # type: ignore[attr-defined]


def convert_timestamp_to_datetime_fromtimestamp_format(dt: date | str | float) -> float:
    """
    Converts a timestamp (secs, ms, μs, ns) to datetime.fromtimestamp() method expected format in seconds

    Args:
        dt [Union[datetime.date, str, float, int]]: datetime.date or timestamp in seconds,
                                                    milliseconds, microseconds or nanoseconds

    Returns:
        float: timestamp in seconds
    """
    dt = str(dt).replace(".", "", 1)
    denominator = len(dt) - 10
    return float(int(dt) / int("".join([str(1), "".join(["0" for _ in range(denominator)])])))


def does_datetime_isoformat_contain_timezone(dt: str) -> bool:
    """
    Checks if datetime string (ISO format) contain timezone information.

    Args:
        dt (str): Datetime string (ISO format)

    Returns:
        bool: contains timezone information.
    """
    validation_regex = [
        ".*[0-9]+(Z)$",  # matches: 2011-11-04T00:05:23Z
        ".*([+-][0-9]+:[0-9]+)$",  # matches: 2011-11-04T00:05:23+04:00, 2011-11-04T00:05:23-04:00
    ]
    return any(re.match(regex, dt) for regex in validation_regex)


def is_json(data):
    """
    Checks if data is valid JSON.

    Args:
        data (str): data to be evaluated.

    Returns:
        bool: contains valid JSON content.
    """
    try:
        json.loads(data)
    # the purpose of capturing exceptions is to control the behavior when something bad happened while reading
    # JSON document - catching general `Exception` to achieve that goal
    except Exception:
        logger.debug("Data is not valid JSON", exc_info=True)
        return False

    return True


def generate_job_status_detection_time_now() -> str:
    return str(datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"))


def extract_structured_data_from_text(
    data: str | list[str],
    regexes: list[str],
    stop_after_first_match: bool = True,
    include_data: bool = True,
) -> list[dict[str, str]]:
    """
    Extract structured data from text using regular expressions with named capture groups.

    Args:
        data (Union[str, List[str]]): The input text data to parse. Can be a single string or a list of strings.
            Each string is processed independently.

        regexes (List[str]): List of regular expression patterns to apply to each input string.
            Each regex should contain at least one named capture group (for example, '(?P<name>pattern)').

        stop_after_first_match (bool): stop processing regexes after the first successful match for each input string
            Defaults to True.

        include_data (bool): include the original input string in the result dictionary under the key 'data'.
            Defaults to True.

    Returns:
        List[Dict[str, str]]: A list of dictionaries containing the extracted data.
            Each dictionary contains:
                - The named capture groups from the regex matches
                - The original input string under the key 'data' (if include_data=True)
    """
    results = []

    if not isinstance(data, list):
        data = [data]

    for event in data:
        found_match = False
        regex_dict: dict[str, str] = {}
        if include_data:
            regex_dict["data"] = event

        for regex in regexes:
            match = re.match(regex, event)
            if match:
                found_match = True
                regex_dict |= match.groupdict()
                if stop_after_first_match:
                    break

        if found_match:
            results.append(regex_dict)

    return results


def find_matching_regex_workflows(config: WatcherConfig, event: str):
    """
    Given a WatcherConfig and a string, call extract_regex_queries (recursive),
    match the string against all regex patterns, and return a list of dicts
    {"name": ..., "type": ...} for all entries with a match. No duplicates.

    Args:
        config (WatcherConfig): The config object.
        event (str): The string to match against all regex patterns.

    Returns:
        List[dict]: List of dicts with keys 'name' and 'type' for all matches.
    """
    results = []
    seen = set()
    for entry in config.extract_regex_queries:
        for pattern in entry["regex"]:
            if re.search(pattern, event):
                key = (entry["name"], entry["type"])
                if key not in seen:
                    results.append({"name": entry["name"], "type": entry["type"]})
                    seen.add(key)

                break
    return results


class LabelRemapper:
    """Efficient backend-field remapping to Event attributes.

    Pre-compiles templates at initialization for optimal performance in high-throughput
    hot paths. Supports both simple 1:1 remapping and composite template-based remapping.

    Args:
        simple: Dict mapping attribute_name -> provider field for 1:1 remapping.
        composite: List of dicts with 'attribute', 'template', and the legacy
            'labels' key containing provider field names.

    Example:
        >>> remapper = LabelRemapper(
        ...     simple={'job_id': 'slurm_job_id'},
        ...     composite=[{
        ...         'attribute': 'log_file_path',
        ...         'template': '/var/log/{job_name}/{job_id}',
        ...         'labels': ['job_name', 'job_id']
        ...     }]
        ... )
        >>> fields = {'slurm_job_id': '12345', 'job_name': 'train', 'job_id': '999'}
        >>> remapper.apply(fields)
        {'job_id': '12345', 'log_file_path': '/var/log/train/999'}
    """

    __slots__ = ("_composite", "_simple")

    def __init__(
        self,
        simple: dict[str, str] | None = None,
        composite: list[dict[str, Any]] | None = None,
    ):
        # Store simple remapping as-is (attribute -> label)
        self._simple: dict[str, str] = simple or {}

        # Pre-process composite templates for fast application
        # Store as list of (attribute, template_str, label_keys)
        self._composite: list[tuple[str, str, list[str]]] = []
        for item in composite or []:
            self._composite.append(
                (
                    item["attribute"],
                    item["template"],
                    item.get("labels", []),
                )
            )

    def apply(self, fields: dict[str, Any]) -> dict[str, Any]:
        """Apply remapping to provider fields, returning new attributes.

        Args:
            fields: Query-visible provider fields.

        Returns:
            Dict with remapped attributes. Only includes mappings where
            all required source fields are present.
        """
        result: dict[str, Any] = {}

        # Simple remapping: attribute_name -> provider field
        for attr_name, label_key in self._simple.items():
            if label_key in fields:
                result[attr_name] = fields[label_key]
            else:
                logger.warning(
                    f"[LabelRemapper] Simple remap attribute '{attr_name}' requires label "
                    f"'{label_key}' which is not present in event fields: {list(fields.keys())}"
                )

        # Composite remapping: template-based
        for attr_name, template, required_labels in self._composite:
            # Check all required labels exist
            missing = [field for field in required_labels if field not in fields]
            if not missing:
                try:
                    result[attr_name] = template.format(**fields)
                except KeyError:
                    pass
            else:
                logger.warning(
                    f"[LabelRemapper] Composite remap attribute '{attr_name}' requires labels "
                    f"{required_labels} but missing: {missing} from event fields: {list(fields.keys())}"
                )

        return result

    @property
    def is_empty(self) -> bool:
        """Check if remapper has any mappings configured."""
        return not self._simple and not self._composite


def apply_label_remapping(
    attributes: dict[str, Any],
    fields: dict[str, Any],
    remapper: LabelRemapper,
) -> dict[str, Any]:
    """Apply label remapping to attributes dict (remapped values take precedence).

    Args:
        attributes: Existing attributes dict to update.
        fields: Query-visible provider fields.
        remapper: Pre-configured LabelRemapper instance.

    Returns:
        Updated attributes dict with remapped values merged in.
        Remapped values override existing attributes.
    """
    remapped = remapper.apply(fields)
    if remapped:
        attributes.update(remapped)
    return attributes


class EventDeduplicator:
    """Thread-safe event deduplicator with time-window duplicate detection.

    Uses regex patterns to extract a "signature" from events, then tracks
    seen signatures in a TTL cache. Events with the same signature within the time
    window are considered duplicates.

    Args:
        window_seconds: Time window for deduplication. Events with the same signature
            within this window are duplicates. Default: 60 seconds.
        max_size: Maximum number of signatures to track. Default: 10000.

    Example - Basic Usage::

        dedup = EventDeduplicator()

        # First occurrence - not a duplicate
        dedup.is_duplicate(content="error occurred")  # Returns False

        # Same content within window - duplicate
        dedup.is_duplicate(content="error occurred")  # Returns True

    Example - Deduplicating Multi-Rank Log Events::

        # These log lines from different ranks should be deduplicated:
        #   329: [2025-12-01 15:34:59,257] [ALWAYS] state=aborted
        #   69:  [2025-12-01 15:34:58,316] [ALWAYS] state=aborted

        dedup = EventDeduplicator()
        strip_patterns = [
            r'^\\d+:\\s*',                                 # Strip rank prefix ("329: ", "69: ")
            r'\\[\\d{4}-\\d{2}-\\d{2}[\\s\\d:,]+\\]\\s*',  # Strip timestamp
        ]
        context = {
            "workflow_step": "nested_restarter",
            "routing_suffix": "CRASH",
            "job_id": "12345",
            "log_file_name": "job.log",
        }

        # Both lines produce same signature: "[ALWAYS] state=aborted"
        dedup.is_duplicate(
            content="329: [2025-12-01 15:34:59,257] [ALWAYS] state=aborted",
            context=context,
            strip_patterns=strip_patterns,
        )  # Returns False (first seen)

        dedup.is_duplicate(
            content="69: [2025-12-01 15:34:58,316] [ALWAYS] state=aborted",
            context=context,
            strip_patterns=strip_patterns,
        )  # Returns True (duplicate)

    Example - Extracting Signature with Pattern::

        # Extract only the state value from logs
        dedup = EventDeduplicator()
        sig_pattern = r'state=(\\w+)'  # Capture group extracts state value

        dedup.is_duplicate(
            content="[INFO] Process state=running pid=1234",
            signature_pattern=sig_pattern,
        )  # Signature: "running"

    Example - YAML Configuration (workflow step)::

        # In workflow config YAML:
        - name: nested_restarter_regex_parse
          type: regex_parser
          attributes:
            dedup: True
            dedup_strip_patterns:
              - '^\\d+:\\s*'                           # Strip rank prefix
              - '\\[\\d{4}-\\d{2}-\\d{2}\\s[\\d:,]+\\]\\s*'  # Strip timestamp
            # Optional: extract specific portion
            # dedup_signature_pattern: 'state=(\\w+)'

    Note:
        - Context fields (workflow_step, routing_suffix, job_id, log_file_name)
          namespace signatures to prevent cross-contamination between jobs/steps.
        - Patterns are compiled and cached for performance.
        - Thread-safe for concurrent access.
    """

    def __init__(
        self,
        window_seconds: int = EVENT_DEDUP_DEFAULT_WINDOW_SECS,
        max_size: int = EVENT_DEDUP_DEFAULT_MAX_SIZE,
    ):
        self._max_size = max_size
        self._seen: TTLCache = TTLCache(maxsize=max_size, ttl=window_seconds)
        self._lock = threading.Lock()
        self._pattern_cache: dict[str, re.Pattern] = {}

    def _get_compiled_pattern(self, pattern: str) -> re.Pattern:
        """Get compiled regex pattern, using cache for efficiency."""
        if pattern not in self._pattern_cache:
            self._pattern_cache[pattern] = re.compile(pattern)

        return self._pattern_cache[pattern]

    def _build_full_signature(
        self,
        content: str,
        context: dict[str, str] | None = None,
        signature_pattern: str | None = None,
        strip_patterns: list[str] | None = None,
    ) -> str:
        """Build full namespaced signature from content and context."""
        sig = content
        signature_pattern_matched = False
        strip_patterns_matched = False

        # Apply signature pattern to extract relevant portion
        if signature_pattern:
            match = self._get_compiled_pattern(signature_pattern).search(content)
            if match:
                sig = match.group(1) if match.lastindex else match.group(0)
                signature_pattern_matched = True

        # Apply strip patterns to remove variable fields
        for pattern in strip_patterns or []:
            compiled = self._get_compiled_pattern(pattern)
            if compiled.search(sig):
                strip_patterns_matched = True

            sig = compiled.sub("", sig)

        # Warn if both patterns were provided but neither matched
        if signature_pattern and strip_patterns and not signature_pattern_matched and not strip_patterns_matched:
            logger.warning(
                f"EventDeduplicator: neither signature_pattern nor strip_patterns matched content. "
                f"signature_pattern={signature_pattern!r}, strip_patterns={strip_patterns!r}, "
                f"content={content}"
            )

        sig = sig.strip()

        # Build namespaced key: context fields + signature (None → "")
        ctx = context or {}
        namespace = ":".join(
            [
                str(ctx.get("workflow_step") or ""),
                str(ctx.get("routing_suffix") or ""),
                str(ctx.get("job_id") or ""),
                str(ctx.get("log_file_name") or ""),
            ]
        )
        return f"{namespace}:{sig}" if namespace.replace(":", "") else sig

    def is_duplicate(
        self,
        content: str,
        context: dict[str, str] | None = None,
        signature_pattern: str | None = None,
        strip_patterns: list[str] | None = None,
    ) -> bool:
        """Check if content is a duplicate within the time window.

        Args:
            content: The event content to check (e.g., event.body or event.to_json()).
            context: Optional dict with keys like workflow_step, routing_suffix, job_id,
                     log_file_name to namespace the deduplication. None values treated as "".
            signature_pattern: Optional regex with capture group to extract signature.
            strip_patterns: Optional list of regex patterns to strip from content.

        Returns:
            True if content with same signature was seen within the time window.
        """
        full_sig = self._build_full_signature(content, context, signature_pattern, strip_patterns)
        sig_hash = sha256(full_sig.encode()).hexdigest()

        with self._lock:
            if sig_hash in self._seen:
                return True
            self._seen[sig_hash] = True
            return False

    def get_signature(
        self,
        content: str,
        context: dict[str, str] | None = None,
        signature_pattern: str | None = None,
        strip_patterns: list[str] | None = None,
    ) -> str:
        """Extract and return the full signature for debugging/testing."""
        return self._build_full_signature(content, context, signature_pattern, strip_patterns)

    @property
    def size(self) -> int:
        """Return current number of entries in the cache."""
        with self._lock:
            return len(self._seen)

    @property
    def max_size(self) -> int:
        """Return maximum cache size."""
        return self._max_size

    @property
    def utilization(self) -> float:
        """Return cache utilization as a percentage (0.0 to 100.0)."""
        return (self.size / self._max_size) * 100.0 if self._max_size > 0 else 0.0


def deduplicate_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate records by (timestamp, body).

    Multiple collector replicas watching the same log file can produce
    identical entries in different provider streams. This collapses them,
    keeping the first occurrence.

    Args:
        records: Parsed log records (each with 'timestamp' and 'body' keys).

    Returns:
        Deduplicated list preserving original order.
    """
    seen: set[tuple[str, str]] = set()
    unique: list[dict[str, Any]] = []
    for rec in records:
        key = (rec.get("timestamp", ""), rec.get("body", ""))
        if key not in seen:
            seen.add(key)
            unique.append(rec)
    return unique
