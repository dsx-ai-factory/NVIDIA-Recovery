# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import heapq
import threading
import time
from typing import Any

from cachetools import LRUCache


class VariableTTLLRUCache(LRUCache):
    """LRU cache with per-item TTLs and optional default TTL.

    This cache extends the standard LRUCache to support individual time-to-live (TTL)
    values for each cached item, while maintaining LRU eviction policy for size-based
    constraints.

    Attributes:
        _default_ttl: Default TTL applied when no TTL is specified for an item.
        _timer: Timer function for getting current time (default: time.monotonic).
        _purge_threshold: Maximum heap size before forced purging is triggered.
        _lock: Threading lock for ensuring thread safety.
        _expires: Dictionary mapping keys to their expiration timestamps.
        _heap: Min-heap of (expiry_time, key) tuples for efficient expiry tracking.

    Example:
        Basic usage with default TTL:
        >>> cache = VariableTTLLRUCache(maxsize=100, default_ttl=300)
        >>> cache['key1'] = 'value1'  # Uses default 300s TTL
        >>> cache.set('key2', 'value2', ttl=60)  # Custom 60s TTL
        >>> cache.ttl('key1')  # Check remaining TTL
        299.8

        Per-item TTL configuration:
        >>> cache['session'] = session_data  # Uses default TTL
        >>> cache.set('temp', temp_data, ttl=30)  # 30 second TTL
        >>> cache.set('permanent', data, ttl=None)  # No time expiry
    """

    def __init__(
        self,
        maxsize: int,
        default_ttl: float | None = None,
        *,
        timer=time.monotonic,
        getsizeof=None,
        purge_threshold: int = 1000,
    ):
        """Initialize the VariableTTLLRUCache.

        Args:
            maxsize: Maximum number of items the cache can hold.
            default_ttl: Default time-to-live in seconds for items when no TTL is
                specified. If None, items without explicit TTL don't expire by time.
            timer: Function to get current time (default: time.monotonic).
            getsizeof: Function to calculate size of cache items (inherited from LRUCache).
            purge_threshold: Maximum number of entries in the expiry heap before
                triggering lazy purging (default: 1000).
        """
        super().__init__(maxsize=maxsize, getsizeof=getsizeof)
        self._default_ttl = default_ttl
        self._timer = timer
        self._purge_threshold = purge_threshold
        # Thread safety lock
        self._lock = threading.RLock()
        # key -> expiry float (monotonic time), or None for "no time expiry"
        self._expires: dict[Any, float] = {}
        # min-heap of (expiry, key); may contain stale entries which are skipped on purge
        self._heap: list[tuple[float, Any]] = []

    def _now(self) -> float:
        """Get the current time using the configured timer function.

        Returns:
            Current timestamp as a float.
        """
        return self._timer()

    def _deadline(self, ttl: float | None) -> float | None:
        """Calculate the expiration deadline for a given TTL.

        Args:
            ttl: Time-to-live in seconds. If None, uses default_ttl or no expiry.

        Returns:
            Expiration timestamp, or None if the item should never expire by time.
            TTL values <= 0 result in immediate expiration (current time).
        """
        if ttl is None:
            if self._default_ttl is None:
                return None
            ttl = self._default_ttl
        # ttl <= 0 means expire immediately
        return self._now() + float(ttl)

    def _should_purge(self) -> bool:
        """Determine if purging is necessary based on lazy purging criteria.

        Uses heuristics to decide when expired items should be cleaned up to avoid
        excessive overhead while preventing memory leaks from stale heap entries.

        Returns:
            True if purging should be performed, False otherwise. Purging is triggered
            when either:
            1. Heap size exceeds the configured purge_threshold, or
            2. Heap size is more than double the cache size (indicates many stale entries)
        """
        heap_size = len(self._heap)
        cache_size = len(self)

        return heap_size >= self._purge_threshold or (cache_size > 0 and heap_size > cache_size * 2)

    def _purge(self) -> None:
        """Remove expired items using the expiry heap.

        Efficiently processes the min-heap to remove all expired items. Skips stale
        heap entries that no longer correspond to valid cache items or have been
        updated with new expiration times.

        Note:
            This method assumes the caller already holds self._lock for thread safety.
            It directly modifies internal cache structures.
        """
        now = self._now()
        heap = self._heap
        expires = self._expires
        data = self._Cache__data  # type: ignore[attr-defined]  # OrderedDict inside LRUCache

        # Clean up expired items from heap
        while heap and heap[0][0] <= now:
            exp, key = heapq.heappop(heap)
            # Skip if this heap entry is stale or the key was evicted/removed
            cur_exp = expires.get(key)
            if cur_exp is None or cur_exp != exp:
                continue
            # Expiration is current; remove it
            expires.pop(key, None)
            if key in data:
                # Use base deletion to keep LRU book-keeping consistent
                super().__delitem__(key)

    def _set_expiry(self, key: Any, deadline: float | None) -> None:
        """Set expiry deadline for a key atomically.

        Updates both the expiration tracking dictionary and heap for the given key.
        If deadline is None, removes any existing expiration for the key.

        Args:
            key: Cache key to set expiration for.
            deadline: Expiration timestamp, or None to remove time-based expiry.

        Note:
            This method assumes the caller already holds self._lock for thread safety.
        """
        if deadline is None:
            # "No time expiry" — remove any previous deadline
            self._expires.pop(key, None)
        else:
            self._expires[key] = deadline
            heapq.heappush(self._heap, (deadline, key))

    def _lazy_purge(self) -> None:
        """Conditionally purge expired items based on lazy purging criteria.

        Only triggers purging when heuristics indicate it's beneficial, avoiding
        unnecessary overhead while preventing excessive memory usage from stale
        heap entries.
        """
        if self._should_purge():
            self._purge()

    def __setitem__(self, key, value, ttl: float | None = None):
        """Set cache item with optional per-item TTL.

        Stores a key-value pair in the cache with an optional time-to-live. If the
        cache is at capacity, LRU eviction policy is applied. Performs lazy purging
        to clean up expired items when beneficial.

        Args:
            key: Cache key.
            value: Value to store.
            ttl: Time-to-live in seconds. If None, uses default_ttl. If default_ttl
                is also None, the item won't expire by time (only by LRU eviction).

        Example:
            >>> cache['key1'] = 'value1'  # Uses default TTL
            >>> cache.__setitem__('key2', 'value2', ttl=60)  # 60 second TTL
            >>> cache.__setitem__('key3', 'value3', ttl=None)  # No time expiry
        """
        with self._lock:
            # Lazy purge to free space from expired entries
            self._lazy_purge()
            deadline = self._deadline(ttl)
            # Delegate actual storage (handles LRU order & size-based eviction)
            super().__setitem__(key, value)
            # Set/refresh the key's expiry deadline atomically
            self._set_expiry(key, deadline)

    def set(self, key, value, *, ttl: float | None = None):
        """Set cache item with keyword-only TTL parameter.

        Convenience method that enforces TTL as a keyword-only argument for clarity.

        Args:
            key: Cache key.
            value: Value to store.
            ttl: Time-to-live in seconds as keyword-only argument.

        Example:
            >>> cache.set('session_key', session_data, ttl=1800)
        """
        self.__setitem__(key, value, ttl=ttl)  # type: ignore[call-arg]

    def __getitem__(self, key):
        """Retrieve cache item with automatic expiry checking.

        Gets an item from the cache, automatically checking if it has expired
        and removing it if necessary. Performs lazy purging when beneficial.

        Args:
            key: Cache key to retrieve.

        Returns:
            The cached value associated with the key.

        Raises:
            KeyError: If the key is not in the cache or has expired.

        Example:
            >>> value = cache['my_key']
            >>> print(value)
        """
        with self._lock:
            # Lazy purge only when needed
            self._lazy_purge()

            # Check hot-path expiry before returning
            exp = self._expires.get(key)
            if exp is not None and exp <= self._now():
                # Expired: delete and raise KeyError like a normal cache miss
                self._expires.pop(key, None)
                try:
                    super().__delitem__(key)
                except KeyError:
                    pass  # Already removed
                raise KeyError(key)

            return super().__getitem__(key)

    def __contains__(self, key) -> bool:
        """Check if key exists in cache with expiry checking.

        Tests for the presence of a key in the cache, automatically removing
        expired items. This is called by the 'in' operator.

        Args:
            key: Cache key to check.

        Returns:
            True if the key exists and hasn't expired, False otherwise.

        Example:
            >>> if 'my_key' in cache:
            ...     print("Key exists")
        """
        with self._lock:
            # Lazy purge only when needed
            self._lazy_purge()

            exp = self._expires.get(key)
            if exp is not None and exp <= self._now():
                # Lazily purge this single key
                self._expires.pop(key, None)
                try:
                    super().__delitem__(key)
                except KeyError:
                    pass  # Already removed
                return False

            return super().__contains__(key)

    def pop(self, key, default=...):
        """Remove and return cache item with expiry handling.

        Removes a key from the cache and returns its value. If the key doesn't
        exist or has expired, returns the default value or raises KeyError.

        Args:
            key: Cache key to remove.
            default: Value to return if key is not found. If not provided,
                KeyError is raised for missing keys.

        Returns:
            The value that was stored under the key.

        Raises:
            KeyError: If key is not found and no default is provided.

        Example:
            >>> value = cache.pop('my_key')
            >>> value = cache.pop('missing_key', 'default_value')
        """
        with self._lock:
            self._lazy_purge()

            # Check if key exists and is not expired
            if key in self:
                self._expires.pop(key, None)
                return super().pop(key)

            if default is ...:
                raise KeyError(key)

            return default

    def __delitem__(self, key) -> None:
        """Delete cache item with expiry cleanup.

        Removes a key-value pair from the cache and cleans up associated
        expiry metadata.

        Args:
            key: Cache key to delete.

        Raises:
            KeyError: If the key is not in the cache.

        Example:
            >>> del cache['my_key']
        """
        with self._lock:
            self._lazy_purge()
            self._expires.pop(key, None)
            super().__delitem__(key)

    def popitem(self):
        """Remove and return least recently used cache item.

        Removes the least recently used item from the cache according to LRU
        policy and cleans up its expiry metadata.

        Returns:
            A tuple of (key, value) for the removed item.

        Raises:
            KeyError: If the cache is empty.

        Example:
            >>> key, value = cache.popitem()
        """
        with self._lock:
            self._lazy_purge()
            key, value = super().popitem()
            self._expires.pop(key, None)
            return key, value

    def clear(self) -> None:
        """Remove all items from the cache.

        Clears both the cached data and all expiry tracking metadata.

        Example:
            >>> cache.clear()
            >>> len(cache)
            0
        """
        with self._lock:
            self._expires.clear()
            self._heap.clear()
            super().clear()

    # Optional helpers
    def ttl(self, key) -> float | None:
        """Get remaining time-to-live for a cache key.

        Returns the number of seconds until the key expires, or None if the key
        has no time-based expiry (only subject to LRU eviction).

        Args:
            key: Cache key to check TTL for.

        Returns:
            Remaining TTL in seconds, or None if no time-based expiry is set.

        Raises:
            KeyError: If the key is not present in the cache or has already expired.

        Example:
            >>> cache.set('temp_key', 'value', ttl=300)
            >>> cache.ttl('temp_key')
            299.8
            >>> cache.set('permanent_key', 'value', ttl=None)
            >>> cache.ttl('permanent_key')
            None
        """
        with self._lock:
            self._lazy_purge()

            if key not in self:
                raise KeyError(key)

            exp = self._expires.get(key)
            if exp is None:
                return None

            remaining = exp - self._now()
            if remaining <= 0:
                # Ensure lazy expiry is reflected
                self._expires.pop(key, None)
                try:
                    super().__delitem__(key)
                except KeyError:
                    pass  # Already removed
                raise KeyError(key)

            return remaining

    def force_purge(self) -> None:
        """Force immediate purging of all expired items.

        Bypasses lazy purging heuristics and immediately removes all expired
        items from the cache. Useful for testing, memory cleanup, or when you
        need guaranteed cleanup regardless of purging thresholds.

        Example:
            >>> cache.force_purge()  # Clean up all expired items now
            >>> len(cache)  # Only non-expired items remain
        """
        with self._lock:
            self._purge()
