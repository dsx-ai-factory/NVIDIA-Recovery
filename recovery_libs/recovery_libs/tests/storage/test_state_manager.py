# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from recovery_libs.storage.state_manager import KeyValueCache, KeyValueCacheStorage


@pytest.fixture
def mock_connection():
    mock = MagicMock()
    mock.session.return_value.__enter__.return_value = MagicMock()
    mock.ready.return_value = True
    mock.bootstrap.return_value = True
    return mock


@pytest.fixture
def cache_record():
    return KeyValueCache(key="job_id", value="123456", ttl_secs=60, timestamp=datetime.now(UTC))


@pytest.fixture
def storage(mock_connection):
    return KeyValueCacheStorage(connection=mock_connection)


def test_cache_creation_defaults():
    now = datetime.now(UTC)
    dc = KeyValueCache(key="k", value="v", timestamp=now)
    assert dc.key == "k"
    assert dc.value == "v"
    assert dc.timestamp == now
    assert dc.ttl_secs == 3600


def test_cache_to_json(cache_record):
    data = KeyValueCache.to_json(cache_record)
    assert data["key"] == "job_id"
    assert data["value"] == "123456"
    assert isinstance(data["timestamp"], str)
    assert data["ttl_secs"] == 60


def test_cache_from_json():
    now = datetime.now(UTC)
    data = {
        "key": "job_id",
        "value": "123456",
        "timestamp": now.isoformat(),
        "ttl_secs": 42,
    }
    dc = KeyValueCache.from_json(data)
    assert dc.key == "job_id"
    assert dc.value == "123456"
    assert dc.timestamp == now
    assert dc.ttl_secs == 42


def test_cache_from_json_defaults():
    now = datetime.now(UTC)
    data = {"key": "job_id", "value": "123456", "timestamp": now.isoformat()}
    dc = KeyValueCache.from_json(data)
    assert dc.key == "job_id"
    assert dc.value == "123456"
    assert dc.timestamp == now
    assert dc.ttl_secs == 3600


def test_storage_ready(storage):
    assert storage.ready() is True


def test_storage_bootstrap(storage):
    assert storage.bootstrap() is True


def test_add_key(storage, cache_record):
    session = storage.connection.session.return_value.__enter__.return_value
    session.add.return_value = None
    session.commit.return_value = None
    assert storage.add_key(cache_record) is True


def test_upsert_key_insert(storage, cache_record):
    session = storage.connection.session.return_value.__enter__.return_value
    session.execute.return_value.scalar_one_or_none.return_value = None
    session.add.return_value = None
    session.commit.return_value = None
    assert storage.upsert_key(cache_record) is True


def test_upsert_key_update(storage, cache_record):
    session = storage.connection.session.return_value.__enter__.return_value
    session.execute.return_value.scalar_one_or_none.return_value = cache_record
    session.commit.return_value = None
    assert storage.upsert_key(cache_record) is True


def test_get_non_expired_keys(storage):
    session = storage.connection.session.return_value.__enter__.return_value
    session.execute.return_value.scalars.return_value.all.return_value = ["k1", "k2", "k1"]
    keys = storage.get_non_expired_keys()
    assert set(keys) == {"k1", "k2"}


def test_get_newest_key(storage, cache_record):
    session = storage.connection.session.return_value.__enter__.return_value
    session.execute.return_value.scalars.return_value.first.return_value = cache_record
    result = storage.get_newest_key("job_id")
    assert result == cache_record


def test_get_oldest_key(storage, cache_record):
    session = storage.connection.session.return_value.__enter__.return_value
    session.execute.return_value.scalars.return_value.first.return_value = cache_record
    result = storage.get_oldest_key("job_id")
    assert result == cache_record


def test_get_non_expired_keys_count(storage):
    session = storage.connection.session.return_value.__enter__.return_value
    session.execute.return_value.scalars.return_value.all.return_value = [1, 2, 3]
    result = storage.get_non_expired_keys_count("job_id")
    assert result == 3


def test_expire_oldest_key(storage, cache_record):
    storage.purge_expired_keys = MagicMock(return_value=0)
    session = storage.connection.session.return_value.__enter__.return_value
    session.execute.return_value.scalars.return_value.first.return_value = cache_record
    session.delete.return_value = None
    session.commit.return_value = None
    assert storage.expire_oldest_key("job_id") is True
