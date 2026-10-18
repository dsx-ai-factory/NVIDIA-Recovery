# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Integration tests for key-value cache deletion and expiry against MySQL."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from recovery_libs.storage.state_manager import KeyValueCache, KeyValueCacheStorage
from sqlalchemy import select


@pytest.fixture
def cache(storage) -> KeyValueCacheStorage:
    return KeyValueCacheStorage(connection=storage.connection)


def _unique_key(name: str) -> str:
    return f"{name}-{uuid.uuid4().hex}"


def _add(cache: KeyValueCacheStorage, key: str, age: timedelta, ttl_secs: int) -> None:
    cache.add_key(KeyValueCache(key=key, value="v", timestamp=datetime.now(UTC) - age, ttl_secs=ttl_secs))


def _stored_keys(cache: KeyValueCacheStorage, keys: list[str]) -> list[str]:
    with cache.connection.session() as session:
        return list(session.scalars(select(KeyValueCache.key).where(KeyValueCache.key.in_(keys))).all())


def test_delete_key_removes_only_matching_records(cache):
    target, other = _unique_key("target"), _unique_key("other")
    _add(cache, target, timedelta(0), ttl_secs=3600)
    _add(cache, target, timedelta(0), ttl_secs=3600)
    _add(cache, other, timedelta(0), ttl_secs=3600)

    assert cache.delete_key(target) == 2
    assert _stored_keys(cache, [target, other]) == [other]


def test_purge_expired_keys_removes_only_expired_records(cache):
    expired, live = _unique_key("expired"), _unique_key("live")
    _add(cache, expired, timedelta(hours=2), ttl_secs=60)
    _add(cache, expired, timedelta(minutes=5), ttl_secs=60)
    _add(cache, live, timedelta(minutes=5), ttl_secs=3600)

    assert cache.purge_expired_keys() == 2
    assert _stored_keys(cache, [expired, live]) == [live]
