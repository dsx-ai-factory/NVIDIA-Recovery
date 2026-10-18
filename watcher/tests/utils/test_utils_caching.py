# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import threading
import time
from unittest.mock import Mock

import pytest
from watcher.lib.utils.caching import VariableTTLLRUCache


class TestVariableTTLLRUCache:
    """Comprehensive test suite for VariableTTLLRUCache."""

    def test_basic_functionality_without_ttl(self):
        """Test basic cache operations without TTL."""
        cache = VariableTTLLRUCache(maxsize=3)

        # Basic set/get
        cache["key1"] = "value1"
        assert cache["key1"] == "value1"
        assert "key1" in cache
        assert len(cache) == 1

        # Multiple items
        cache["key2"] = "value2"
        cache["key3"] = "value3"
        assert len(cache) == 3

        # Delete
        del cache["key1"]
        assert "key1" not in cache
        assert len(cache) == 2

    def test_default_ttl_functionality(self):
        """Test cache with default TTL."""
        cache = VariableTTLLRUCache(maxsize=5, default_ttl=0.1)

        cache["key1"] = "value1"
        assert cache["key1"] == "value1"

        # Wait for expiration
        time.sleep(0.15)

        # Should be expired
        with pytest.raises(KeyError):
            _ = cache["key1"]
        assert "key1" not in cache

    def test_per_item_ttl_using_setitem(self):
        """Test per-item TTL using __setitem__ with ttl parameter."""
        cache = VariableTTLLRUCache(maxsize=5, default_ttl=1.0)

        # Short TTL item
        cache.__setitem__("short", "value_short", ttl=0.1)
        # Long TTL item
        cache.__setitem__("long", "value_long", ttl=1.0)

        assert cache["short"] == "value_short"
        assert cache["long"] == "value_long"

        # Wait for short TTL to expire
        time.sleep(0.15)

        # Short should be expired, long should still exist
        with pytest.raises(KeyError):
            _ = cache["short"]
        assert cache["long"] == "value_long"

    def test_per_item_ttl_using_set_method(self):
        """Test per-item TTL using the set() method."""
        cache = VariableTTLLRUCache(maxsize=5, default_ttl=1.0)

        # Use set method with ttl
        cache.set("test_key", "test_value", ttl=0.1)
        assert cache["test_key"] == "test_value"

        # Wait for expiration
        time.sleep(0.15)

        with pytest.raises(KeyError):
            _ = cache["test_key"]

    def test_none_ttl_with_default(self):
        """Test None TTL falls back to default TTL."""
        cache = VariableTTLLRUCache(maxsize=5, default_ttl=0.1)

        cache.__setitem__("key", "value", ttl=None)
        assert cache["key"] == "value"

        # Should expire using default TTL
        time.sleep(0.15)
        with pytest.raises(KeyError):
            _ = cache["key"]

    def test_none_ttl_without_default(self):
        """Test None TTL without default means no expiration."""
        cache = VariableTTLLRUCache(maxsize=5, default_ttl=None)

        cache.__setitem__("permanent", "value", ttl=None)
        assert cache["permanent"] == "value"

        # Should never expire by time
        time.sleep(0.1)
        assert cache["permanent"] == "value"

    def test_zero_and_negative_ttl(self):
        """Test zero and negative TTL values (immediate expiration)."""
        cache = VariableTTLLRUCache(maxsize=5)

        # Zero TTL - should expire immediately
        cache.__setitem__("zero_ttl", "value", ttl=0)
        with pytest.raises(KeyError):
            _ = cache["zero_ttl"]

        # Negative TTL - should also expire immediately
        cache.__setitem__("neg_ttl", "value", ttl=-1)
        with pytest.raises(KeyError):
            _ = cache["neg_ttl"]

    def test_lru_eviction(self):
        """Test LRU eviction behavior."""
        cache = VariableTTLLRUCache(maxsize=2)

        cache["key1"] = "value1"
        cache["key2"] = "value2"

        # Access key1 to make it more recently used
        _ = cache["key1"]

        # Add third item, should evict key2 (least recently used)
        cache["key3"] = "value3"

        assert "key1" in cache
        assert "key2" not in cache
        assert "key3" in cache

    def test_ttl_method(self):
        """Test the ttl() method for checking remaining time."""
        cache = VariableTTLLRUCache(maxsize=5)

        cache.__setitem__("test", "value", ttl=1.0)

        # Check remaining TTL
        remaining = cache.ttl("test")
        assert remaining is not None
        assert 0.5 < remaining <= 1.0

        # Test key with no TTL
        cache.__setitem__("no_ttl", "value", ttl=None)
        assert cache.ttl("no_ttl") is None

        # Test expired key
        cache.__setitem__("expired", "value", ttl=0.01)
        time.sleep(0.02)
        with pytest.raises(KeyError):
            cache.ttl("expired")

    def test_contains_with_expiry(self):
        """Test __contains__ method with expiry logic."""
        cache = VariableTTLLRUCache(maxsize=5)

        cache.__setitem__("test", "value", ttl=0.1)
        assert "test" in cache

        # Wait for expiration
        time.sleep(0.15)
        assert "test" not in cache

    def test_pop_method(self):
        """Test pop() method with expiry handling."""
        cache = VariableTTLLRUCache(maxsize=5)

        cache["key1"] = "value1"
        cache.__setitem__("key2", "value2", ttl=0.1)

        # Pop existing key
        value = cache.pop("key1")
        assert value == "value1"
        assert "key1" not in cache

        # Pop with default
        default_value = cache.pop("nonexistent", "default")
        assert default_value == "default"

        # Pop without default should raise KeyError
        with pytest.raises(KeyError):
            cache.pop("nonexistent")

        # Wait for key2 to expire, then try to pop
        time.sleep(0.15)
        with pytest.raises(KeyError):
            cache.pop("key2")

    def test_popitem_method(self):
        """Test popitem() method (LRU eviction)."""
        cache = VariableTTLLRUCache(maxsize=5)

        cache["key1"] = "value1"
        cache["key2"] = "value2"

        # Access key1 to make it more recently used
        _ = cache["key1"]

        # popitem should remove least recently used (key2)
        key, value = cache.popitem()
        assert key == "key2"
        assert value == "value2"
        assert "key2" not in cache
        assert "key1" in cache

    def test_clear_method(self):
        """Test clear() method."""
        cache = VariableTTLLRUCache(maxsize=5, default_ttl=1.0)

        cache["key1"] = "value1"
        cache.__setitem__("key2", "value2", ttl=0.5)

        assert len(cache) == 2

        cache.clear()

        assert len(cache) == 0
        assert "key1" not in cache
        assert "key2" not in cache

    def test_custom_timer(self):
        """Test cache with custom timer function."""
        mock_timer = Mock()

        # Start at time 0
        mock_timer.return_value = 0.0
        cache = VariableTTLLRUCache(maxsize=5, default_ttl=1.0, timer=mock_timer)

        # Set item at time 0 (expires at time 1.0)
        cache["key"] = "value"

        # Access at time 0.5 (not expired)
        mock_timer.return_value = 0.5
        assert cache["key"] == "value"

        # Move time to 1.5 (item should be expired)
        mock_timer.return_value = 1.5
        cache.force_purge()

        # Item should now be expired
        with pytest.raises(KeyError):
            _ = cache["key"]

    def test_heap_cleanup_with_stale_entries(self):
        """Test that heap cleanup handles stale entries correctly."""
        cache = VariableTTLLRUCache(maxsize=5)

        # Add items with different TTLs
        cache.__setitem__("key1", "value1", ttl=0.1)
        cache.__setitem__("key2", "value2", ttl=0.2)
        cache.__setitem__("key3", "value3", ttl=0.3)

        # Manually delete one item to create stale heap entry
        del cache["key2"]

        # Wait for remaining items to expire
        time.sleep(0.35)

        # Force purge to clean up expired items
        cache.force_purge()

        # All items should now be cleaned up
        assert len(cache) == 0

    def test_overwrite_key_with_different_ttl(self):
        """Test overwriting a key with different TTL."""
        cache = VariableTTLLRUCache(maxsize=5)

        # Set with short TTL
        cache.__setitem__("key", "value1", ttl=0.1)

        # Immediately overwrite with longer TTL
        cache.__setitem__("key", "value2", ttl=1.0)

        # Wait past original TTL
        time.sleep(0.15)

        # Should still exist with new value and TTL
        assert cache["key"] == "value2"

    def test_thread_safety_basic(self):
        """Basic thread safety test."""
        cache = VariableTTLLRUCache(maxsize=100, default_ttl=1.0)
        results = []
        exceptions = []

        def worker(thread_id):
            try:
                for i in range(10):
                    key = f"thread_{thread_id}_key_{i}"
                    cache[key] = f"value_{i}"
                    if key in cache:
                        value = cache[key]
                        results.append((thread_id, key, value))
            except Exception as e:  # noqa: BLE001
                exceptions.append(e)

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

    def test_memory_cleanup_on_expiry(self):
        """Test that expired items are properly cleaned from internal structures."""
        cache = VariableTTLLRUCache(maxsize=5)

        # Add items that will expire
        for i in range(3):
            cache.__setitem__(f"key_{i}", f"value_{i}", ttl=0.1)

        # Check internal structures have entries
        assert len(cache._expires) == 3
        assert len(cache._heap) == 3

        # Wait for expiration
        time.sleep(0.15)

        # Force purge to clean up expired items
        cache.force_purge()

        # Internal structures should be cleaned
        assert len(cache._expires) == 0
        assert len(cache) == 0

    def test_edge_case_empty_cache_operations(self):
        """Test operations on empty cache."""
        cache = VariableTTLLRUCache(maxsize=5)

        # Test operations on empty cache
        assert len(cache) == 0
        assert "nonexistent" not in cache

        with pytest.raises(KeyError):
            _ = cache["nonexistent"]

        with pytest.raises(KeyError):
            cache.ttl("nonexistent")

        with pytest.raises(KeyError):
            cache.pop("nonexistent")

        with pytest.raises(KeyError):
            cache.popitem()

    def test_large_cache_performance(self):
        """Test performance with larger cache size."""
        cache = VariableTTLLRUCache(maxsize=1000, default_ttl=1.0)

        # Add many items
        start_time = time.time()
        for i in range(500):
            cache[f"key_{i}"] = f"value_{i}"

        # Access items
        for i in range(0, 500, 10):
            _ = cache[f"key_{i}"]

        end_time = time.time()

        # Should complete reasonably quickly (less than 1 second)
        assert end_time - start_time < 1.0
        assert len(cache) == 500

    def test_integration_lru_and_ttl(self):
        """Test integration of LRU eviction and TTL expiry."""
        cache = VariableTTLLRUCache(maxsize=2)

        # Add items: one with TTL, one without
        cache.__setitem__("ttl_key", "ttl_value", ttl=0.2)
        cache.__setitem__("no_ttl_key", "no_ttl_value", ttl=None)

        # Add third item to trigger LRU eviction
        cache["new_key"] = "new_value"

        # ttl_key should be evicted by LRU
        assert "ttl_key" not in cache
        assert "no_ttl_key" in cache
        assert "new_key" in cache

        # Wait for any remaining TTL expiry
        time.sleep(0.25)

        # no_ttl_key should still exist (no TTL)
        assert "no_ttl_key" in cache

    def test_lazy_purging_behavior(self):
        """Test that lazy purging works correctly."""
        # Use a low purge threshold to demonstrate lazy behavior
        cache = VariableTTLLRUCache(maxsize=100, purge_threshold=5)

        # Add items that will expire quickly
        for i in range(3):
            cache.__setitem__(f"key_{i}", f"value_{i}", ttl=0.01)

        # Wait for expiration
        time.sleep(0.02)

        # Items should still be in cache due to lazy purging
        assert len(cache) == 3  # Items not yet purged
        assert len(cache._heap) == 3  # Heap still has entries

        # Add more items to trigger purge threshold
        for i in range(3, 8):
            cache.__setitem__(f"key_{i}", f"value_{i}", ttl=10.0)

        # Now accessing should trigger purge
        _ = cache["key_3"]  # This should trigger lazy purge

        # Expired items should now be cleaned up
        assert len(cache) == 5  # Only non-expired items remain
        assert "key_0" not in cache
        assert "key_1" not in cache
        assert "key_2" not in cache
        assert "key_3" in cache

    def test_force_purge_method(self):
        """Test the force_purge method."""
        cache = VariableTTLLRUCache(maxsize=5)

        # Add items with short TTL
        cache.__setitem__("key1", "value1", ttl=0.01)
        cache.__setitem__("key2", "value2", ttl=0.01)

        time.sleep(0.02)

        # Items should still exist due to lazy purging
        assert len(cache) == 2

        # Force purge should clean them up immediately
        cache.force_purge()

        assert len(cache) == 0
        assert len(cache._expires) == 0
