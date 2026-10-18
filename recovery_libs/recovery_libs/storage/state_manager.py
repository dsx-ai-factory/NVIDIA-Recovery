# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any, ClassVar

from sqlalchemy import Index, Integer, String, delete, func, select, text
from sqlalchemy.orm import Mapped, mapped_column

from recovery_libs.storage.base import RecoveryStorageException
from recovery_libs.storage.dataclass import DataClassBase
from recovery_libs.storage.mysql_connection import MySQLConnection
from recovery_libs.storage.type_decorators import TZDateTimeType

logger = logging.getLogger(__name__)


class KeyValueCache(DataClassBase):
    """
    ORM-compatible class for caching records with a key, value, timestamp, and TTL.
    """

    __tablename__ = "key_value_cache"
    non_init_fields: ClassVar[set[str]] = {"id"}

    # Primary key
    id: Mapped[int] = mapped_column(init=False, kw_only=True, primary_key=True)

    # Required fields
    key: Mapped[str] = mapped_column(String(256), kw_only=True)
    value: Mapped[str] = mapped_column(String(2048), kw_only=True)
    timestamp: Mapped[datetime] = mapped_column(TZDateTimeType, kw_only=True)
    ttl_secs: Mapped[int] = mapped_column(Integer, default=3600, kw_only=True)

    # Additional configurations
    __table_args__ = (
        Index("key_index", "key"),
        Index("timestamp_index", "timestamp"),
    )

    @classmethod
    def to_json(cls, obj: "KeyValueCache") -> dict[str, Any]:
        """Convert a KeyValueCache instance to a JSON-compatible dictionary."""
        return {
            "key": obj.key,
            "value": obj.value,
            "timestamp": obj.timestamp.isoformat(),
            "ttl_secs": obj.ttl_secs,
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "KeyValueCache":
        """Create a KeyValueCache instance from a JSON-compatible dictionary."""
        return cls(
            key=data["key"],
            value=data["value"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            ttl_secs=data.get("ttl_secs", 3600),
        )


class KeyValueCacheStorage:
    """
    KeyValueCacheStorage provides a cache-like data structure backed by MySQL, where each key-value pair (record) has a time-to-live (TTL) after which it is considered expired and is no longer returned by cache queries.

    This class is intended to simulate the behavior of an in-memory cache (such as Redis or Memcached) but persists data in a relational database. Each record has a key, value, timestamp, and TTL (in seconds). Records are considered valid (non-expired) if the current time is less than or equal to timestamp + ttl_secs. Expired records can be purged from the database.

    Methods:
        - add_key(record): Add a new key to the cache. Does not check for duplicates.
        - upsert_key(record): Insert or update a key in the cache. If the key exists, update its value, timestamp, and TTL; otherwise, add a new record.
        - delete_key(key): Delete all records with the given key. Returns the number of deleted records.
        - get_non_expired_keys(): Return a list of unique keys that are not expired (timestamp + ttl_secs >= now).
        - get_newest_key(key): Return the most recent non-expired record for the given key, or None if not found.
        - get_oldest_key(key): Return the oldest non-expired record for the given key, or None if not found.
        - get_non_expired_keys_count(key): Return the number of non-expired records for the given key.
        - expire_oldest_key(key): Purge all expired records, then delete the oldest non-expired record for the given key. Returns True if a record was deleted, False otherwise.
        - purge_expired_keys(): Delete all expired records from the cache. Returns the number of deleted records.
        - ready(): Return True if the underlying storage is ready.
        - bootstrap(schema_upgrade=True): Initialize or upgrade the underlying database schema.
        - read_records(...): Read records with optional filters for key and timestamp, with pagination.
    """

    def __init__(self, connection: MySQLConnection):
        super().__init__()
        self.connection = connection

    def ready(self) -> bool:
        """Return true if the detector cache storage is ready"""
        return self.connection.ready()

    def bootstrap(self, schema_upgrade: bool = True) -> bool:
        """Bootstrap the detector cache storage either using the legacy code path or the new schema upgrade code path."""
        return self.connection.bootstrap(schema_upgrade=schema_upgrade)

    def read_records(
        self,
        key: str | None = None,
        timestamp: datetime | None = None,
        offset: int = 0,
        limit: int = 1000,
    ) -> Sequence[KeyValueCache]:
        """Read records with the given parameters"""
        if not key and not timestamp:
            raise ValueError("At least one of key or timestamp is required")

        try:
            with self.connection.session() as session:
                select_query = select(KeyValueCache)
                if key:
                    select_query = select_query.where(KeyValueCache.key == key)
                if timestamp:
                    select_query = select_query.where(KeyValueCache.timestamp == timestamp)
                select_query = select_query.offset(offset).limit(limit)
                return session.scalars(select_query).all()
        except Exception as e:
            logger.exception("Failed to retrieve detector cache records", extra={"error": str(e)})
            raise RecoveryStorageException(f"Failed to retrieve detector cache records: {e}") from e

    def add_key(self, record: KeyValueCache) -> bool:
        """Add a new key in the detector cache record."""
        try:
            with self.connection.session() as session:
                session.expire_on_commit = False
                session.add(record)
                session.commit()
                logger.debug(f"Added new detector cache key: {record}")
                return True
        except Exception as e:
            logger.exception("Failed to add new detector cache key", extra={"error": str(e)})
            raise RecoveryStorageException(f"Failed to add new detector cache key: {e}") from e

    def upsert_key(self, record: KeyValueCache) -> bool:
        """Upsert a key in the detector cache. If the key exists, update the record; otherwise, add a new record."""
        try:
            with self.connection.session() as session:
                session.expire_on_commit = False
                existing = session.execute(
                    select(KeyValueCache).where(KeyValueCache.key == record.key)
                ).scalar_one_or_none()
                if existing:
                    existing.value = record.value
                    existing.timestamp = record.timestamp
                    existing.ttl_secs = record.ttl_secs
                    session.commit()
                    logger.debug(f"Updated detector cache key: {record.key}")
                else:
                    session.add(record)
                    session.commit()
                    logger.debug(f"Created new detector cache key: {record}")
                return True
        except Exception as e:
            logger.exception("Failed to add or update detector cache key", extra={"error": str(e)})
            raise RecoveryStorageException(f"Failed to add or update detector cache key: {e}") from e

    def delete_key(self, key: str) -> int:
        """Delete all records where key equals the provided key. Returns the number of deleted records."""
        try:
            with self.connection.session() as session:
                session.expire_on_commit = False
                result = session.connection().execute(delete(KeyValueCache).where(KeyValueCache.key == key))
                session.commit()
                count = result.rowcount
                logger.debug(f"Deleted {count} detector cache key(s) for key: {key}")
                return count
        except Exception as e:
            logger.exception(
                f"Failed to delete detector cache key(s) for key {key}", extra={"key": key, "error": str(e)}
            )
            raise RecoveryStorageException(f"Failed to delete detector cache key(s) for key {key}: {e}") from e

    def get_non_expired_keys(self) -> list[str]:
        """Return a list of unique keys where timestamp + ttl_secs >= datetime.now()."""
        try:
            now = datetime.now(UTC)
            with self.connection.session() as session:
                valid_keys = (
                    session.execute(
                        select(KeyValueCache.key).where(
                            func.timestampadd(text("SECOND"), KeyValueCache.ttl_secs, KeyValueCache.timestamp) >= now
                        )
                    )
                    .scalars()
                    .all()
                )
                unique_keys = list(set(valid_keys))
                logger.debug(f"Fetched {len(unique_keys)} non-expired detector cache key(s): {unique_keys}")
                return unique_keys
        except Exception as e:
            logger.exception("Failed to get non-expired detector cache keys", extra={"error": str(e)})
            raise RecoveryStorageException(f"Failed to get non-expired detector cache keys: {e}") from e

    def purge_expired_keys(self) -> int:
        """Delete all records where timestamp + ttl_secs < datetime.now(). Returns the number of deleted records."""
        try:
            now = datetime.now(UTC)
            with self.connection.session() as session:
                result = session.connection().execute(
                    delete(KeyValueCache).where(
                        func.timestampadd(text("SECOND"), KeyValueCache.ttl_secs, KeyValueCache.timestamp) < now
                    )
                )
                session.commit()
                count = result.rowcount
                logger.debug(f"Purged {count} expired detector cache keys.")
                return count
        except Exception as e:
            logger.exception("Failed to purge expired detector cache keys", extra={"error": str(e)})
            raise RecoveryStorageException(f"Failed to purge expired detector cache keys: {e}") from e

    def get_newest_key(self, key: str) -> KeyValueCache | None:
        """Return the non-expired record with the most recent timestamp for the given key, or None if not found."""
        try:
            now = datetime.now(UTC)
            with self.connection.session() as session:
                record = (
                    session.execute(
                        select(KeyValueCache)
                        .where(
                            (KeyValueCache.key == key)
                            & (
                                func.timestampadd(text("SECOND"), KeyValueCache.ttl_secs, KeyValueCache.timestamp)
                                >= now
                            )
                        )
                        .order_by(KeyValueCache.timestamp.desc())
                    )
                    .scalars()
                    .first()
                )
                logger.debug(f"Fetched newest non-expired detector cache key for key '{key}': {record}")
                return record
        except Exception as e:
            logger.exception(
                f"Failed to get newest non-expired detector cache key for key '{key}'",
                extra={"key": key, "error": str(e)},
            )
            raise RecoveryStorageException(
                f"Failed to get newest non-expired detector cache key for key '{key}': {e}"
            ) from e

    def get_oldest_key(self, key: str) -> KeyValueCache | None:
        """Return the non-expired record with the oldest timestamp for the given key, or None if not found."""
        try:
            now = datetime.now(UTC)
            with self.connection.session() as session:
                record = (
                    session.execute(
                        select(KeyValueCache)
                        .where(
                            (KeyValueCache.key == key)
                            & (
                                func.timestampadd(text("SECOND"), KeyValueCache.ttl_secs, KeyValueCache.timestamp)
                                >= now
                            )
                        )
                        .order_by(KeyValueCache.timestamp.asc())
                    )
                    .scalars()
                    .first()
                )
                logger.debug(f"Fetched oldest non-expired detector cache key for key '{key}': {record}")
                return record
        except Exception as e:
            logger.exception(
                f"Failed to get oldest non-expired detector cache key for key '{key}'",
                extra={"key": key, "error": str(e)},
            )
            raise RecoveryStorageException(
                f"Failed to get oldest non-expired detector cache key for key '{key}': {e}"
            ) from e

    def get_non_expired_keys_count(self, key: str) -> int:
        """Return the number of non-expired records for the given key."""
        try:
            now = datetime.now(UTC)
            with self.connection.session() as session:
                count = (
                    session.execute(
                        select(KeyValueCache).where(
                            (KeyValueCache.key == key)
                            & (
                                func.timestampadd(text("SECOND"), KeyValueCache.ttl_secs, KeyValueCache.timestamp)
                                >= now
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                result = len(count)
                logger.debug(f"Found {result} non-expired detector cache record(s) for key '{key}'")
                return result
        except Exception as e:
            logger.exception(
                f"Failed to count non-expired detector cache records for key '{key}'",
                extra={"key": key, "error": str(e)},
            )
            raise RecoveryStorageException(
                f"Failed to count non-expired detector cache records for key '{key}': {e}"
            ) from e

    def expire_oldest_key(self, key: str) -> bool:
        """Purge all expired records, then delete the non-expired record with the oldest timestamp for the given key. Returns True if deleted, False if not found."""
        try:
            purged = self.purge_expired_keys()
            logger.debug(f"Purged {purged} expired detector cache record(s) before expiring oldest for key '{key}'")
            now = datetime.now(UTC)
            with self.connection.session() as session:
                session.expire_on_commit = False
                record = (
                    session.execute(
                        select(KeyValueCache)
                        .where(
                            (KeyValueCache.key == key)
                            & (
                                func.timestampadd(text("SECOND"), KeyValueCache.ttl_secs, KeyValueCache.timestamp)
                                >= now
                            )
                        )
                        .order_by(KeyValueCache.timestamp.asc())
                    )
                    .scalars()
                    .first()
                )
                if record:
                    session.delete(record)
                    session.commit()
                    logger.debug(f"Deleted oldest non-expired detector cache key for key '{key}': {record}")
                    return True
                else:
                    logger.debug(f"No non-expired detector cache key found to expire for key '{key}'")
                    return False
        except Exception as e:
            logger.exception(
                f"Failed to expire oldest non-expired detector cache key for key '{key}'",
                extra={"key": key, "error": str(e)},
            )
            raise RecoveryStorageException(
                f"Failed to expire oldest non-expired detector cache key for key '{key}': {e}"
            ) from e
