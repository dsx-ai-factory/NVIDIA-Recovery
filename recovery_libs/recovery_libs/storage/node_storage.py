# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import logging
import uuid
from collections import deque
from collections.abc import Sequence
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import joinedload

from recovery_libs.storage.base import RecoveryStorageException
from recovery_libs.storage.job_execution_errors import JobExecutionError
from recovery_libs.storage.mysql_connection import MySQLConnection
from recovery_libs.storage.node_events import NodeEvent

logger = logging.getLogger(__name__)


class NodeStorage:
    """
    Class to write and read the node-centric tables (NodeEvents and Errors), currently using MySQL
    """

    def __init__(self, connection: MySQLConnection):
        super().__init__()
        self.connection = connection

    def bootstrap(self, schema_upgrade: bool = True) -> bool:
        return self.connection.bootstrap(schema_upgrade=schema_upgrade) and self.ready()

    def ready(self) -> bool:
        return self.connection.ready()

    def store_attribution(self, error: JobExecutionError, node_events: list[NodeEvent]) -> bool:
        """Create a new record"""
        try:
            existing_error = self.read_error(error)
            if not existing_error:
                logger.debug("Creating new error:%s", error)
                error = self.insert_error(error)
            else:
                logger.debug("Found existing error with id: %s", existing_error.id)
                error = existing_error
            with self.connection.session() as session:
                session.expire_on_commit = False
                for node_event in node_events:
                    node_event.job_execution_error_id = error.id
                    session.add(node_event)
                session.commit()
                logger.debug("Succesfully created error attribution: %s", node_events)
                return True
        except Exception as e:
            logger.exception("Failed to create error attribution", extra={"error": str(e)})
            raise RecoveryStorageException(f"Failed to create error attribution: {e}") from e

    def read_error(self, error: JobExecutionError) -> JobExecutionError | None:
        """Read an error record that is identical to the given error record"""
        try:
            with self.connection.session() as session:
                session.expire_on_commit = False
                return (
                    session.query(JobExecutionError)
                    .filter(
                        JobExecutionError.code == error.code,
                        JobExecutionError.signature == error.signature,
                        JobExecutionError.message == error.message,
                        JobExecutionError.description == error.description,
                    )
                    .first()
                )
        except Exception as e:
            logger.exception("Failed to read identical error", extra={"error": str(e)})
            raise RecoveryStorageException(f"Failed to read identical error: {e}") from e

    def insert_error(self, error: JobExecutionError) -> JobExecutionError:
        """Insert a new error record"""
        try:
            with self.connection.session() as session:
                session.add(error)
                session.commit()
                session.refresh(error)
                logger.debug("Succesfully created error with id: %s", error.id)
                return error
        except Exception as e:
            logger.exception("Failed to insert error", extra={"error": str(e)})
            raise RecoveryStorageException(f"Failed to insert error: {e}") from e

    def read_node_attributions_for_job(
        self,
        job_id: str,
        job_attempt_index: int | None = None,
        anomaly_uuid: uuid.UUID | None = None,
    ) -> Sequence[NodeEvent]:
        """
        Read all node error attributions for a given job.
        If the job_attempt_index is not provided, the function will return all the node error attributions for the given job.
        If provided but -1, the function will return the node error attribution for the last job attempt.
        Args:
            job_id: The id of the job
            job_attempt_index: The attempt index of the job, should be non negative, -1 for the last job attempt,
            other values are invalid.
            anomaly_uuid: The optional job anomaly id
        Returns:
            A sequence of node events where each event has an error attribution.
        """
        if job_attempt_index is not None and job_attempt_index < -1:
            raise ValueError(
                f"job_attempt_index {job_attempt_index} should be non negative, -1 for the last job attempt, other values are invalid."
            )
        try:
            with self.connection.session() as session:
                session.expire_on_commit = False
                select_query = select(NodeEvent).options(joinedload(NodeEvent.job_execution_error))
                select_query = select_query.where(NodeEvent.job_id == job_id)
                if job_attempt_index is not None and job_attempt_index >= 0:
                    select_query = select_query.where(NodeEvent.job_attempt_index == job_attempt_index)
                if anomaly_uuid:
                    select_query = select_query.where(NodeEvent.job_anomaly_id == anomaly_uuid)
                select_query = select_query.order_by(NodeEvent.job_attempt_index.desc(), NodeEvent.timestamp.desc())
                # Valid or missing job_attempt_index, return all events
                if job_attempt_index is None or job_attempt_index >= 0:
                    return session.scalars(select_query).all()
                # return all events with the same job_attempt_index as the last one,
                else:
                    ret: deque[NodeEvent] = deque()
                    for node_event in session.scalars(select_query).all():
                        if not ret or ret[0].job_attempt_index == node_event.job_attempt_index:
                            ret.appendleft(node_event)
                        else:
                            break
                    return list(ret)
        except Exception as e:
            logger.exception(
                "Failed to retrieve error attributions for job %s",
                job_id,
                extra={"job_id": job_id, "error": str(e)},
            )
            raise RecoveryStorageException(f"Failed to retrieve error attributions for job {job_id}, due to error: {e}")

    def read_attributions(
        self,
        node_names: list[str] | None = None,
        job_ids: list[str] | None = None,
        start_timestamp: datetime | None = None,
        end_timestamp: datetime | None = None,
        anomaly_uuid: uuid.UUID | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[Sequence[NodeEvent], int]:
        """
        Read error attributions for the given nodes and job ids in the given time range.
        Args:
            node_names: The names of the nodes
            job_ids: The ids of the jobs
            start_timestamp: The optional start time of the time range, inclusive
            end_timestamp: The optional end time of the time range, exclusive
            anomaly_uuid: The optional job anomaly id
            limit: The maximum number of records to return
            offset: The offset of the first record to return
        Returns:
            A sequence of node events where each event has an error attribution.
        """
        if not node_names and not job_ids and not start_timestamp and not end_timestamp and not anomaly_uuid:
            raise ValueError("At least one filtering criteria must be provided.")
        try:
            with self.connection.session() as session:
                session.expire_on_commit = False
                select_query = select(NodeEvent).options(joinedload(NodeEvent.job_execution_error))
                select_count_query = select(func.count(NodeEvent.id))
                if node_names:
                    select_query = select_query.where(NodeEvent.node.in_(node_names))
                    select_count_query = select_count_query.where(NodeEvent.node.in_(node_names))
                if job_ids:
                    select_query = select_query.where(NodeEvent.job_id.in_(job_ids))
                    select_count_query = select_count_query.where(NodeEvent.job_id.in_(job_ids))
                if start_timestamp is not None:
                    select_query = select_query.where(NodeEvent.timestamp >= start_timestamp)
                    select_count_query = select_count_query.where(NodeEvent.timestamp >= start_timestamp)
                if end_timestamp is not None:
                    select_query = select_query.where(NodeEvent.timestamp < end_timestamp)
                    select_count_query = select_count_query.where(NodeEvent.timestamp < end_timestamp)
                if anomaly_uuid:
                    select_query = select_query.where(NodeEvent.job_anomaly_id == anomaly_uuid)
                    select_count_query = select_count_query.where(NodeEvent.job_anomaly_id == anomaly_uuid)

                select_query = select_query.limit(limit).offset(offset).order_by(NodeEvent.timestamp.desc())

                record_count = session.scalars(select_count_query).first() or 0
                records = session.scalars(select_query).all()

                return (records, record_count)
        except Exception as e:
            logger.exception("Failed to retrieve error attributions", extra={"error": str(e)})
            raise RecoveryStorageException(f"Failed to retrieve error attributions: {e}") from e
