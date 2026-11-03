# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import json
import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum, IntEnum
from typing import Any, ClassVar, Optional

from pydantic import ValidationError
from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    inspect,
)
from sqlalchemy.dialects.mysql import VARCHAR as MySQLVARCHAR
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    mapped_column,
    relationship,
)

from recovery_libs.schema.aps_spec.spec import ApsSlurmJobSpec, AutoResumeModeEnum
from recovery_libs.schema.job_manager.models import (
    Issue,
    JobAnomaly,
    JobCheckpointInfo,
    JobTimeFilterTarget,
)
from recovery_libs.storage.type_decorators import (
    ApsSlurmJobSpecType,
    IntEnumListType,
    IssuesType,
    JobAnomaliesType,
    JobCheckpointsType,
    TZDateTimeMicrosecondsType,
    TZDateTimeType,
)
from recovery_libs.utils.slurm import comment_is_APS, parse_nodelist

# FIXME - this should not be done here
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def validate_timestamp(ts: int | dict, now: datetime) -> datetime | None:
    """
    Check that the UNIX timestamp is 'valid'. Return None if
    invalid, otherwise convert to a UTC datetime.

    Times that are 0 are invalid, and times that are in the future are
    invalid. 'Future' in this case is some provided datetime (`now`).

    NB: Slurm may choose to report a future time in some cases based
    on estimated scheduling, but we are not interested in this.
    """
    seconds = ts.get("number", -1) if isinstance(ts, dict) else ts

    if seconds > 0:
        stamp = datetime.fromtimestamp(seconds, UTC)
        if stamp < now:
            return stamp
    return None


def parse_resource_value_sacct(
    typekey: str,
    vals: dict[str, Any],
    namekey: str | None = None,
) -> int | None:
    """
    Slurm sacct reports resources in lists of {"type": "<typekey>", "name": "<name>"
    "count": <num>}.  Here, we search for key in such a list and
    return the count. If anything goes wrong, we return None. We optionally
    will look at the name too if it's there.
    """
    try:
        parse_target = []
        if "allocated" in vals:
            # The "allocated" dictionary contains no nested dictionaries.
            parse_target = vals["allocated"]
        elif "requested" in vals:
            # The "requested" dictionary contains nested dictionaries like "max", "min", "average", "total", etc.
            # Here we just use the value from the "total" section.
            parse_target = vals["requested"].get("total", [])
        else:
            logger.warning("Unable to find 'allocated' or 'requested' key in sacct resource lists.")
            return None

        for entry in parse_target:
            if entry["type"] == typekey:
                if namekey is not None and entry["name"] != namekey:
                    continue
                return entry["count"]
    except KeyError:
        logger.debug(f"Unable to parse {typekey}:{namekey} resource from sacct resource lists: {parse_target}.")
    return None


def parse_job_state(state: str | list[str], choose_final_state: bool = False) -> str:
    """
    Parse the job state from sacct/squeue JSON output. In different versions of Slurm, the value can be a plain string
    or a list of string. This function handles both scenario and returns a string value.

    choose_final_state is a flag to indicate whether to choose the final state from the list of states when multiple
    state values are present. If no final state is found, it will fall back to the first state in the list.
    """

    if isinstance(state, str):
        return state
    elif isinstance(state, list):
        if not state:
            raise ValueError("Invalid Slurm JSON output: when state field is a list, it cannot be empty.")

        if choose_final_state:
            for state_item in state:
                if state_item in set(SlurmJobFinalStates):
                    return state_item
        return state[0]
    else:
        raise ValueError("Invalid Slurm JSON output: state field should be either a string or a list of strings.")  # noqa: TRY004


def to_mega(instr: str) -> int:
    """
    Interpret string as 'mega' scale; if it has a suffix in
    (K,M,G,T), convert the prefix to an int and scale to mega.
    If no suffix, convert to int (and we assume mega).

    This is almost always bytes, but we're flexible.
    """
    if instr[-1].isdigit():
        return int(instr)

    match instr[-1]:
        case "K" | "k":
            factor = 1.0 / 1000
        case "M" | "m":
            factor = 1
        case "G" | "g":
            factor = 1000
        case "T" | "t":
            factor = 1000 * 1000
        case _:
            raise ValueError("Unexpected suffix: %s")

    return int(float(instr[:-1]) * factor)


def parse_tres_str(tres_str: str) -> dict[str, int]:
    """
    Parse a string of the form "key=val,key1=val1,etc" into a dictionary of the form
    {
      key: val,
      key1: val1,
      ...
    }
    """

    if not tres_str:
        return {}
    return {k: to_mega(v) for k, v in [kv.split("=") for kv in tres_str.split(",")]}


def extract_resource_value(request_dict: dict[str, int], allocation_dict: dict[str, int], key: str) -> int | None:
    """
    Extract the value of a type of resource from the request and allocation dictionaries.

    Parameters:
        request_dict: The dictionary containing the requested resources.
        allocation_dict: The dictionary containing the actual resource allocation.
        key: The key of the resource to extract, when the key exists in both dictionaries, the value from the allocation
            dictionary is returned.

    Returns:
        The value of the resource if it exists in one of the dictionaries, otherwise None.
    """
    if key in allocation_dict:
        return allocation_dict[key]
    elif key in request_dict:
        return request_dict[key]
    return None


def parse_job_id_from_squeue_json(job_json: dict[str, Any]) -> str | None:
    """
    Parse the Slurm job id from the job JSON dictionary. This function handles two scanrios:
    - When job runs as part of a job array, the job id is in the format of <job_array_id>_<array_index>, e.g., 12345_1.
    - When job is standalone, the job id is directly taken from the "job_id" field.

    For job array elements that are eligible to be scheduled, the "array_job_id" and "array_task_id" sections look like
    below. The array_task_id.set field is true.
    {
      ...
      "array_job_id": {
        "set": true,
        "infinite": false,
        "number": 1632774
      },
      "array_task_id": {
        "set": true,
        "infinite": false,
        "number": 0
      },
      ...
    }

    For job array elements that are not ready to be scheduled, e.g., the array specification is like 0-7%6 which only
    allows 6 out of 8 tasks to be eligible for allocation at any given time. Slurm will just represent the non-eligible
    tasks with a synthetic entry that looks like below, the key difference is that array_task_id.set field is false
    while the array_task_id.number is always 0.
    {
      ...
      "array_job_id": {
        "set": true,
        "infinite": false,
        "number": 1632774
      },
      "array_task_id": {
        "set": false,
        "infinite": false,
        "number": 0
      },
      ...
    }

    We will not convert synthetic entry in to job attempt as it will collide with attempt 0 of task 0.
    """

    if (
        "array_job_id" in job_json
        and int(job_json["array_job_id"].get("number", 0)) > 0
        and "array_task_id" in job_json
        and "number" in job_json["array_task_id"]
    ):
        if not job_json["array_task_id"].get("set", False):
            logger.debug(
                f"Skipping synthetic job array entry with array_job_id={job_json['array_job_id']['number']}. "
                f"array_task_id.set is false, indicating this is not an individually scheduled task."
            )
            return None
        return f"{job_json['array_job_id']['number']}_{job_json['array_task_id']['number']}"
    else:
        logger.debug(f"Job is not part of a job array, parsing job id from job_id field. Job JSON: {job_json}")

    return str(job_json["job_id"])


def parse_job_group_id_from_squeue_json(job_json: dict[str, Any]) -> str | None:
    """
    Parse the Slurm job group id from the job JSON dictionary. The job group id is only present when the job is part
    of a job array. If the job is not part of a job array, None is returned.

    For job array jobs, the "array_job_id" section looks like:
    {
      ...
      "array_job_id": {
        "set": true,
        "infinite": false,
        "number": 1632774
      },
      ...
    }
    """
    if "array_job_id" in job_json and int(job_json["array_job_id"].get("number", 0)) > 0:
        return str(job_json["array_job_id"]["number"])
    else:
        logger.debug(f"Job is not part of a job array, no job group id present. Job JSON: {job_json}")

    return None


def parse_job_id_from_sacct_json(job_json: dict[str, Any]) -> str | None:
    """
    Parse the Slurm job id from the job JSON dictionary. This function handles two scanrios:
    - When job runs as part of a job array, the job id is in the format of <job_array_id>_<array_index>, e.g., 12345_1.
    - When job is standalone, the job id is directly taken from the "job_id" field.

    For job array elements that are eligible to be scheduled, the "array" section looks like below, where the
    array.task_id.set is set to true.
    {
      ...
      "array": {
        "job_id": 1632774,
        "limits": {
          "max": {
            "running": {
              "tasks": 0
            }
          }
        },
        "task_id": {
          "set": true,
          "infinite": false,
          "number": 0
        },
        "task": ""
      },
      ...
    }

    For job array elements that are not eligible to be scheduled, e.g., the array specification is like 0-7%6 which only
    allows 6 out of 8 tasks to be eligible for allocation at any given time. Slurm will just represent the non-eligible
    tasks with a synthetic entry that looks like below, the key difference is that array.task_id.set field is false
    while the array.task_id.number is always 0.
    {
      ...
      "array": {
        "job_id": 1632774,
        "limits": {
          "max": {
            "running": {
              "tasks": 0
            }
          }
        },
        "task_id": {
          "set": false,
          "infinite": false,
          "number": 0
        },
        "task": ""
      },
      ...
    }

    We will not convert synthetic entry in to job attempt as it will collide with attempt 0 of task 0.
    """

    if (
        "array" in job_json
        and int(job_json["array"].get("job_id", 0)) > 0
        and "task_id" in job_json["array"]
        and "number" in job_json["array"]["task_id"]
    ):
        if not job_json["array"]["task_id"].get("set", False):
            logger.debug(
                f"Skipping synthetic job array entry with array.job_id={job_json['array']['job_id']}. "
                f"array.task_id.set is false, indicating this is not an individually scheduled task."
            )
            return None
        return f"{job_json['array']['job_id']}_{job_json['array']['task_id']['number']}"
    else:
        logger.debug("Job is not part of a job array, parsing job id from job_id field.")

    return str(job_json["job_id"])


def parse_job_group_id_from_sacct_json(job_json: dict[str, Any]) -> str | None:
    """
    Parse the Slurm job group id from the job JSON dictionary. The job group id is only present when the job is part
    of a job array. If the job is not part of a job array, None is returned.

    For job array elements in sacct json output, the "array" section looks like:
    {
      ...
      "array": {
        "job_id": 1632774,
        "limits": {
          "max": {
            "running": {
              "tasks": 0
            }
          }
        },
        "task_id": {
          "set": true,
          "infinite": false,
          "number": 0
        },
        "task": ""
      },
      ...
    }
    """
    if "array" in job_json and int(job_json["array"].get("job_id", 0)) > 0:
        return str(job_json["array"]["job_id"])
    else:
        logger.debug(f"Job is not part of a job array, no job group id present. Job JSON: {job_json}")

    return None


class SlurmJobFinalStates(str, Enum):
    """
    Final states for Slurm jobs, taken from official doc:
    https://slurm.schedmd.com/squeue.html#SECTION_JOB-STATE-CODES
    and
    https://slurm.schedmd.com/job_state_codes.html

    Note that although state REQUEUED is implied to be a tranisent state, in reality it can appear to be a final state
    from the sacct response, therefore we regard it as a final state here.
    """

    BOOT_FAIL = "BOOT_FAIL"
    CANCELLED = "CANCELLED"
    COMPLETED = "COMPLETED"
    DEADLINE = "DEADLINE"
    FAILED = "FAILED"
    NODE_FAIL = "NODE_FAIL"
    OUT_OF_MEMORY = "OUT_OF_MEMORY"
    PREEMPTED = "PREEMPTED"
    REVOKED = "REVOKED"
    SPECIAL_EXIT = "SPECIAL_EXIT"
    STOPPED = "STOPPED"
    SUSPENDED = "SUSPENDED"
    TIMEOUT = "TIMEOUT"
    REQUEUED = "REQUEUED"


class SlurmJobNonFinalStates(str, Enum):
    """
    Non final states for Slurm jobs, taken from official doc:
    https://slurm.schedmd.com/squeue.html#SECTION_JOB-STATE-CODES
    and
    https://slurm.schedmd.com/job_state_codes.html
    and
    https://slurm.schedmd.com/job_state_codes.html#flags
    """

    COMPLETING = "COMPLETING"
    CONFIGURING = "CONFIGURING"
    PENDING = "PENDING"
    POWER_UP_NODE = "POWER_UP_NODE"
    RESIZING = "RESIZING"
    RESV_DEL_HOLD = "RESV_DEL_HOLD"
    RUNNING = "RUNNING"
    SIGNALING = "SIGNALING"
    STAGE_OUT = "STAGE_OUT"
    UPDATE_DB = "UPDATE_DB"


class JobConfigValidationFailureTypes(IntEnum):
    """
    The result of job config validation.

    LOG_FILE_PATH_NOT_FOUND: No log file path was found that matches the expected naming convention.
    """

    LOG_FILE_PATH_NOT_FOUND = 1


class DagDependencyActionStatus(str, Enum):
    """
    The status of a DAG dependency action.

    NOT_ELIGIBLE: The dependency trigger condition has not been met yet.
    IN_PROGRESS: The action has been sent to WFC and is awaiting confirmation.
    SUCCESSFUL: The action was executed successfully.
    FAILED: The action failed after exhausting all retries.
    ABORTED: A parent reached terminal Advice, so the dependent action must not run.
    """

    NOT_ELIGIBLE = "not_eligible"
    IN_PROGRESS = "in_progress"
    SUCCESSFUL = "successful"
    FAILED = "failed"
    ABORTED = "aborted"


class DagDependencyReasonCode(str, Enum):
    """Structured explanation for the latest DAG dependency evaluation."""

    WAITING_FOR_PARENT_REGISTRATION = "WAITING_FOR_PARENT_REGISTRATION"
    WAITING_FOR_PARENT_COMPLETION = "WAITING_FOR_PARENT_COMPLETION"
    DEPENDENCY_SATISFIED_WAITING_FOR_OTHERS = "DEPENDENCY_SATISFIED_WAITING_FOR_OTHERS"
    PARENT_NAME_AMBIGUOUS = "PARENT_NAME_AMBIGUOUS"
    PARENT_TERMINATED = "PARENT_TERMINATED"


class Base(DeclarativeBase):
    """
    Base class for all ORM classes.
    """


class DataClassBase(MappedAsDataclass, Base):
    """
    Base class for ORM classes that need be used as Python dataclasses too.
    """

    __abstract__ = True


class JobEvent(Base):
    """
    Sqlalchemy ORM class for Heimdall job events.
    """

    __tablename__ = "job_events"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    job_record_id: Mapped[int] = mapped_column(ForeignKey("job_execution_records.id"), nullable=False)
    job_id: Mapped[str] = mapped_column(String(32))
    job_attempt_index: Mapped[int] = mapped_column(Integer)
    type: Mapped[str] = mapped_column(String(32))
    time: Mapped[datetime] = mapped_column(TZDateTimeType)
    log_file_name: Mapped[str | None] = mapped_column(String(256), default=None)

    # The content that triggered the event.
    payload: Mapped[str] = mapped_column(Text)

    __table_args__ = (
        Index("event_type_index", "type"),
        Index("event_time_index", "time"),
        Index("job_id_attempt_index", "job_id", "job_attempt_index"),
    )

    def __str__(self):
        return (
            f"id: {self.id}, "
            f"job_record_id: {self.job_record_id}, "
            f"job_id: {self.job_id}, "
            f"job_attempt_index: {self.job_attempt_index}, "
            f"event_type: {self.type}, "
            f"event_time: {self.time}, "
            f"log_file_name: {self.log_file_name}, "
            f"payload: {self.payload}, "
        )


@dataclass
class WorkflowRecord:
    """
    Class to represent jobs grouped by their canonical workflow ID.
    """

    id: str
    start_time: datetime | None
    end_time: datetime | None


class SlurmJobExecutionRecord(DataClassBase):
    """
    Class to represent a Slurm job execution in the NVIDIA Recovery service stack.

    This class serves as the internal data model representation and is subject
    to change at anytime without warning. It should only be used within the NVIDIA
    Recovery service stack. An external representation will be derived from this into a
    Pydantic object for consumption through APIs.

    Most of the fields are marked as Optional because at certain point in a job's
    lifecycle, they can be empty. Job id and job attempt index are required to
    uniquely identify a job record.

    This class is also the SQLAlchemy ORM class for the job execution records table. For
    more information about it, please refer to the SQLAlchemy documentation:
    https://docs.sqlalchemy.org/en/20/orm/dataclasses.html.
    """

    ##### SQL Alchemy related class variables #####
    __tablename__ = "job_execution_records"

    # Fields that are mapped to SQL database columns but not exposed in the __init__ method.
    # Mainly to support initializing the object from a dictionary.
    non_init_fields: ClassVar[set[str]] = {"id"}

    ##### Job metadata #####
    id: Mapped[int] = mapped_column(init=False, kw_only=True, primary_key=True)

    heimdall_job_id: Mapped[uuid.UUID | None] = mapped_column(Uuid, default=None, kw_only=True, unique=True)

    # job_id has str type instead of int to be more future-proof.
    job_id: Mapped[str] = mapped_column(String(64), kw_only=True)
    job_attempt_index: Mapped[int] = mapped_column(kw_only=True)
    job_group_id: Mapped[str | None] = mapped_column(String(64), default=None, kw_only=True)

    workflow_id: Mapped[str] = mapped_column(
        MySQLVARCHAR(length=256, charset="utf8mb4", collation="utf8mb4_0900_bin"), kw_only=True
    )
    job_name: Mapped[str | None] = mapped_column(String(256), default=None, kw_only=True)
    # TODO: Change it to enum once all possible states are enumerated.
    state: Mapped[str | None] = mapped_column(String(32), default=None, kw_only=True)
    state_reason: Mapped[str | None] = mapped_column(String(64), default=None, kw_only=True)

    # All timestamp should be in UTC timezone.
    submit_time: Mapped[datetime | None] = mapped_column(TZDateTimeType, default=None, kw_only=True)
    start_time: Mapped[datetime | None] = mapped_column(TZDateTimeType, default=None, kw_only=True)
    end_time: Mapped[datetime | None] = mapped_column(TZDateTimeType, default=None, kw_only=True)
    cluster: Mapped[str | None] = mapped_column(String(64), default=None, kw_only=True)

    # partitioin and reservation are exclusive to SLURM jobs
    partition: Mapped[str | None] = mapped_column(String(64), default=None, kw_only=True)
    reservation: Mapped[str | None] = mapped_column(String(64), default=None, kw_only=True)
    # namespace is exclusive to k8s / run:ai jobs, k8s namespaces can have max length of 253 chars
    namespace: Mapped[str | None] = mapped_column(String(256), default=None, kw_only=True)
    # worker_cluster_name identifies which cluster the job is running on
    worker_cluster_name: Mapped[str | None] = mapped_column(String(64), default=None, kw_only=True)

    user: Mapped[str | None] = mapped_column(String(32), default=None, kw_only=True)
    comment: Mapped[str | None] = mapped_column(Text, default=None, kw_only=True)
    # Command represents the command used to launch the job, e.g. sbatch test.sh
    command: Mapped[str | None] = mapped_column(Text, default=None, kw_only=True)
    dependency: Mapped[str | None] = mapped_column(String(64), default=None, kw_only=True)
    total_task_count: Mapped[int | None] = mapped_column(default=None, kw_only=True)

    ##### Resource related fields #####
    node_count: Mapped[int | None] = mapped_column(default=None, kw_only=True)
    allocated_node_list: Mapped[list[str] | None] = mapped_column(JSON, default=None, kw_only=True)
    inclusion_node_list: Mapped[list[str] | None] = mapped_column(
        JSON, default=None, kw_only=True
    )  # TODO: do we check nodes for taints and include them here?
    exclusion_node_list: Mapped[list[str] | None] = mapped_column(
        JSON, default=None, kw_only=True
    )  # TODO: do we check nodes for taints and include them here?
    sticky_exclusion_node_list: Mapped[list[str] | None] = mapped_column(JSON, default=None, kw_only=True)

    # As of Slurm 24.11, sacct only reports one failed node per job attempt, our data model supports a list of nodes
    # for future compatibility.
    failed_node_list: Mapped[list[str] | None] = mapped_column(JSON, default=None, kw_only=True)

    # Note: here the assumption is that tasks within a job are identical, thus per-task resources can be calculated by
    # dividing total resources with number of tasks.
    total_cpu_count: Mapped[int | None] = mapped_column(default=None, kw_only=True)
    # Slurm reports memory in megabytes.
    total_memory_mb: Mapped[int | None] = mapped_column(default=None, kw_only=True)
    total_gpu_count: Mapped[int | None] = mapped_column(default=None, kw_only=True)

    # Scheduler metadata (e.g., CRD kind, namespace, labels)
    scheduler_metadata: Mapped[dict | None] = mapped_column(JSON, default=None, kw_only=True)

    anomalies: Mapped[list[JobAnomaly] | None] = mapped_column(JobAnomaliesType, default=None, kw_only=True)
    checkpoints: Mapped[list[JobCheckpointInfo] | None] = mapped_column(JobCheckpointsType, default=None, kw_only=True)
    events: Mapped[list[JobEvent]] = relationship(order_by=JobEvent.time, kw_only=True, default_factory=list)

    aps_spec: Mapped[ApsSlurmJobSpec | None] = mapped_column(ApsSlurmJobSpecType, default=None, kw_only=True)

    config_validation_failure_types: Mapped[list[JobConfigValidationFailureTypes] | None] = mapped_column(
        IntEnumListType(JobConfigValidationFailureTypes), default=None, kw_only=True
    )

    ##### Fields for internal tracking #####
    last_state_transition_time: Mapped[datetime | None] = mapped_column(TZDateTimeType, default=None, kw_only=True)
    last_updated_at: Mapped[datetime | None] = mapped_column(TZDateTimeType, kw_only=True)
    # Set once JM has emitted the AttributionRequest event to ADI for this attempt. Used to
    # de-dup emission across the terminal-transition and anomaly-record paths via an
    # atomic conditional update (see claim_attribution_request_emission).
    attribution_request_emitted: Mapped[bool] = mapped_column(Boolean, default=False, kw_only=True)

    # DEPRECATED: this field is no longer used and will not be populated with actual values.
    retry_of: Mapped[str | None] = mapped_column(String(64), default=None, kw_only=True)

    # The absolute log file paths for the job steps so long as the file path is a valid APS path, if no
    # steps, then the parent job log path. These are the paths for which a symlink was created by WFC
    log_file_paths: Mapped[list[str] | None] = mapped_column(JSON, default=None, kw_only=True)

    ##### SQL table additional configurations #####
    __table_args__ = (
        Index("heimdall_job_id_index", "heimdall_job_id"),
        Index("job_id_attempt_unique_index", "job_id", "job_attempt_index", unique=True),
        Index("job_group_id_index", "job_group_id"),
        Index("ix_job_execution_records_workflow_id", "workflow_id"),
        Index("job_name_index", "job_name"),
        Index("job_state_index", "state"),
        Index("user_index", "user"),
        Index("submit_time_index", "submit_time"),
        Index("start_time_index", "start_time"),
        Index("end_time_index", "end_time"),
        CheckConstraint(
            "CHAR_LENGTH(workflow_id) > 0",
            name="ck_job_execution_records_workflow_id_nonempty",
        ),
    )

    @classmethod
    def from_sacct_json(
        cls: type["SlurmJobExecutionRecord"],
        json: dict[str, Any],
        readtime: datetime,
        attempt_index: int,
        workflow_id: str,
    ) -> Optional["SlurmJobExecutionRecord"]:
        """
        Produce a new SlurmJobExecutionRecord based on
        interpreting fields from the parsed json. readtime is
        provided to fill in context about the provenance of the json.

        Returns None for synthetic job array entries that are not individually scheduled.

        Example sacct json: see sacct --json output for the fields used below.
        """

        job_id = parse_job_id_from_sacct_json(json)
        if job_id is None:
            return None

        comment = json.get("comment", {}).get("job", "")
        aps_spec = None
        if comment and (aps_spec_dict := comment_is_APS(comment)):
            try:
                aps_spec = ApsSlurmJobSpec.model_validate(aps_spec_dict["APS"])
            except ValidationError as e:
                logger.warning(
                    f"Validation error occurred while parsing the APS section of Slurm job spec of job {json['job_id']} "
                    f"into ApsSlurmJobSpec class. Exception: {e}"
                )
            except Exception as e:
                logger.exception(
                    f"Unexpected error occurred while parsing APS section of Slurm job spec of job {json['job_id']}.",
                    extra={"job_id": json["job_id"], "error": str(e)},
                )

        job_record = cls(
            job_id=job_id,
            job_attempt_index=attempt_index,
            workflow_id=workflow_id,
            job_group_id=parse_job_group_id_from_sacct_json(json),
            job_name=json["name"],
            state=parse_job_state(state=json["state"]["current"], choose_final_state=True),
            state_reason=json["state"]["reason"],
            submit_time=validate_timestamp(json["time"]["submission"], readtime),
            start_time=validate_timestamp(json["time"]["start"], readtime),
            end_time=validate_timestamp(json["time"]["end"], readtime),
            cluster=json["cluster"],
            partition=json["partition"],
            # this can be a list, I think
            reservation=json["reservation"]["name"],
            # if user is empty, this is explicitly set to None to avoid overriding the value from squeue in the database
            user=json["user"] if json["user"] else None,
            # Command is expliclty set to None to avoid overriding the value from squeue which has the absolute path.
            command=None,  # Optional[str]
            comment=json["comment"].get("job"),
            dependency=json.get("dependency"),  # Optional[str]
            total_task_count=None,  # Optional[int]
            node_count=json["allocation_nodes"],
            allocated_node_list=list(parse_nodelist(json["nodes"])),
            inclusion_node_list=None,  # Optional[List[str]]
            exclusion_node_list=None,  # Optional[List[str]]
            # As of slurm 24.11, sacct only reports one failed node per job attempt, here we convert it to a
            # single-element list when available.
            failed_node_list=[json["failed_node"]] if json.get("failed_node") else None,
            total_cpu_count=parse_resource_value_sacct(typekey="cpu", vals=json["tres"]),
            total_memory_mb=parse_resource_value_sacct(typekey="mem", vals=json["tres"]),
            total_gpu_count=parse_resource_value_sacct(typekey="gres", namekey="gpu", vals=json["tres"]),
            aps_spec=aps_spec,
            last_updated_at=readtime,
            retry_of=None,
            log_file_paths=None,  # Optional[List[str]]
        )

        # If the job's auto_resume_mode is REQUEUE, the state reason from sacct can be misleading and incorrect. Given
        # that when sacct is used for a job, the state_reason must have been set by squeue output already, therefore we
        # don't need to include the state_reason from sacct in this case.
        if aps_spec and aps_spec.auto_resume_mode == AutoResumeModeEnum.REQUEUE:
            job_record.state_reason = None

        return job_record

    @classmethod
    def from_squeue_json(
        cls: type["SlurmJobExecutionRecord"], json: dict[str, Any], readtime: datetime, workflow_id: str
    ) -> Optional["SlurmJobExecutionRecord"]:
        """
        Produce a new SlurmJobExecutionRecord based on
        interpreting fields from the parsed json.  readtime is
        provided to fill in context about the provenance of the json.

        Returns None for synthetic job array entries that are not individually scheduled.
        """

        job_id = parse_job_id_from_squeue_json(json)
        if job_id is None:
            return None

        # TODO: Consider merging the APS-check and parsing into a single function and use it everywhere where
        # comment_is_APS is used.
        comment = json.get("comment", "")
        aps_spec = None
        if comment and (aps_spec_dict := comment_is_APS(comment)):
            try:
                aps_spec = ApsSlurmJobSpec.model_validate(aps_spec_dict["APS"])
            except ValidationError as e:
                logger.warning(
                    f"Validation error occurred while parsing the APS section of Slurm job spec of job {json['job_id']} "
                    f"into ApsSlurmJobSpec class. Exception: {e}"
                )
            except Exception as e:
                logger.warning(
                    f"Unexpected error occurred while parsing APS section of Slurm job spec of job {json['job_id']}.",
                    exc_info=True,
                    extra={"job_id": json["job_id"], "error": str(e)},
                )

        req_dict = parse_tres_str(json["tres_req_str"])
        alloc_dict = parse_tres_str(json["tres_alloc_str"])
        return cls(
            job_id=job_id,
            job_attempt_index=json["restart_cnt"],
            workflow_id=workflow_id,
            job_group_id=parse_job_group_id_from_squeue_json(json),
            job_name=json["name"],
            # Slurm >=23.11 carries the base state and flags together while an
            # epilog runs. Prefer the final state in that array so state-based
            # recovery can overlap COMPLETING; scalar states from older Slurm
            # are unaffected.
            state=parse_job_state(json["job_state"], choose_final_state=True),
            state_reason=json["state_reason"],
            submit_time=validate_timestamp(json["submit_time"], readtime),
            start_time=validate_timestamp(json["start_time"], readtime),
            end_time=validate_timestamp(json["end_time"], readtime),
            cluster=json["cluster"],
            partition=json["partition"],
            # this can be a list, I think
            reservation=json["resv_name"],
            user=_get_user_from_squeue(json),
            command=json.get("command"),
            comment=json["comment"],
            dependency=json["dependency"],  # Optional[str]
            total_task_count=None,  # Optional[int]
            node_count=json["node_count"]["number"],
            allocated_node_list=list(parse_nodelist(json["nodes"])),
            inclusion_node_list=list(parse_nodelist(json["required_nodes"])),
            exclusion_node_list=list(parse_nodelist(json["excluded_nodes"])),
            total_cpu_count=extract_resource_value(req_dict, alloc_dict, "cpu"),
            total_memory_mb=extract_resource_value(req_dict, alloc_dict, "mem"),
            total_gpu_count=extract_resource_value(req_dict, alloc_dict, "gres/gpu"),
            aps_spec=aps_spec,
            last_updated_at=readtime,
            retry_of=None,
            log_file_paths=None,
        )

    @classmethod
    def from_json(cls: type["SlurmJobExecutionRecord"], json: dict[str, Any]) -> "SlurmJobExecutionRecord":
        """
        Produce a new SlurmJobExecutionRecord object from a json dictionary.
        The json dict was originally generated by serializing an
        SlurmJobExecutionRecord object.
        """

        return cls(
            # Explicitly cast job_id into string to avoid job_id to be stored as int accidentally.
            job_id=str(json["job_id"]),
            job_attempt_index=json["job_attempt_index"],
            workflow_id=json["workflow_id"],
            job_group_id=json.get("job_group_id") or None,
            job_name=json["job_name"],
            state=json["state"],
            state_reason=json["state_reason"],
            submit_time=(datetime.fromisoformat(json["submit_time"]) if json["submit_time"] else None),
            start_time=(datetime.fromisoformat(json["start_time"]) if json["start_time"] else None),
            end_time=(datetime.fromisoformat(json["end_time"]) if json["end_time"] else None),
            cluster=json["cluster"],
            partition=json.get("partition"),
            reservation=json.get("reservation"),
            namespace=json.get("namespace"),
            user=json["user"],
            command=json.get("command"),
            comment=json.get("comment"),
            dependency=json["dependency"],
            total_task_count=json["total_task_count"],
            node_count=json["node_count"],
            allocated_node_list=json["allocated_node_list"],
            inclusion_node_list=json["inclusion_node_list"],
            exclusion_node_list=json["exclusion_node_list"],
            sticky_exclusion_node_list=json.get("sticky_exclusion_node_list"),
            failed_node_list=json["failed_node_list"] if json.get("failed_node_list") else None,
            total_cpu_count=json["total_cpu_count"],
            total_memory_mb=json["total_memory_mb"],
            total_gpu_count=json["total_gpu_count"],
            scheduler_metadata=json.get("scheduler_metadata"),
            anomalies=(
                [JobAnomaly.model_validate_json(anomaly) for anomaly in json["anomalies"]]
                if json["anomalies"]
                else None
            ),
            checkpoints=(
                [JobCheckpointInfo.model_validate_json(checkpoint) for checkpoint in json["checkpoints"]]
                if json.get("checkpoints")
                else None
            ),
            aps_spec=(ApsSlurmJobSpec.model_validate_json(json["aps_spec"]) if json.get("aps_spec") else None),
            config_validation_failure_types=(
                [
                    JobConfigValidationFailureTypes(failure_type)
                    for failure_type in json["config_validation_failure_types"]
                ]
                if json.get("config_validation_failure_types")
                else None
            ),
            last_state_transition_time=(
                datetime.fromisoformat(json["last_state_transition_time"])
                if json.get("last_state_transition_time")
                else None
            ),
            last_updated_at=datetime.fromisoformat(json["last_updated_at"]),
            attribution_request_emitted=json.get("attribution_request_emitted", False),
            retry_of=json["retry_of"],
            log_file_paths=(json["log_file_paths"] if json.get("log_file_paths") else None),
            worker_cluster_name=json.get("worker_cluster_name"),
        )

    def to_dict(self) -> dict[str, Any]:
        """
        Serialize the record into a Python dictionary.

        Always prefer calling this to serialize an instance to a dictionary over using dataclasses.asdict() from the
        dataclasses module, especially when reading the records from the database because asdict() isn't aware of lazy
        loading of SQLAlchemy ORM relationship fields properly.
        """

        inspect_result = inspect(self)

        return {
            "job_id": self.job_id,
            "heimdall_job_id": self.heimdall_job_id,
            "job_attempt_index": self.job_attempt_index,
            "workflow_id": self.workflow_id,
            "job_group_id": self.job_group_id,
            "job_name": self.job_name,
            "state": self.state,
            "state_reason": self.state_reason,
            "submit_time": self.submit_time,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "cluster": self.cluster,
            "partition": self.partition,
            "reservation": self.reservation,
            "namespace": self.namespace,
            "user": self.user,
            "command": self.command,
            "comment": self.comment,
            "dependency": self.dependency,
            "total_task_count": self.total_task_count,
            "node_count": self.node_count,
            "allocated_node_list": self.allocated_node_list,
            "inclusion_node_list": self.inclusion_node_list,
            "exclusion_node_list": self.exclusion_node_list,
            "sticky_exclusion_node_list": self.sticky_exclusion_node_list,
            "failed_node_list": self.failed_node_list,
            "total_cpu_count": self.total_cpu_count,
            "total_memory_mb": self.total_memory_mb,
            "total_gpu_count": self.total_gpu_count,
            # Convert None to empty dict to match the schema definition of the external data model.
            "scheduler_metadata": self.scheduler_metadata if self.scheduler_metadata is not None else {},
            "anomalies": self.anomalies,
            "checkpoints": self.checkpoints,
            # Check if the events relationship is loaded to avoid error related to lazy loading especially when the
            # table joins are explicitly disabled for performance considerations.
            "events": self.events if "events" not in inspect_result.unloaded else [],
            "aps_spec": self.aps_spec,
            "config_validation_failure_types": self.config_validation_failure_types,
            "last_state_transition_time": self.last_state_transition_time,
            "last_updated_at": self.last_updated_at,
            "attribution_request_emitted": self.attribution_request_emitted,
            "retry_of": self.retry_of,
            "log_file_paths": self.log_file_paths,
            "filename": None,
            "worker_cluster_name": self.worker_cluster_name,
        }

    def to_json(self) -> str:
        """
        Serialize the record into a JSON string.
        """

        return json.dumps(
            {
                "job_id": self.job_id,
                "job_attempt_index": self.job_attempt_index,
                "workflow_id": self.workflow_id,
                "job_group_id": self.job_group_id,
                "job_name": self.job_name,
                "state": self.state,
                "state_reason": self.state_reason,
                "submit_time": self.submit_time.isoformat() if self.submit_time else None,
                "start_time": self.start_time.isoformat() if self.start_time else None,
                "end_time": self.end_time.isoformat() if self.end_time else None,
                "cluster": self.cluster,
                "partition": self.partition,
                "reservation": self.reservation,
                "namespace": self.namespace,
                "user": self.user,
                "command": self.command,
                "comment": self.comment,
                "dependency": self.dependency,
                "total_task_count": self.total_task_count,
                "node_count": self.node_count,
                "allocated_node_list": self.allocated_node_list,
                "inclusion_node_list": self.inclusion_node_list,
                "exclusion_node_list": self.exclusion_node_list,
                "sticky_exclusion_node_list": self.sticky_exclusion_node_list,
                "failed_node_list": self.failed_node_list,
                "total_cpu_count": self.total_cpu_count,
                "total_memory_mb": self.total_memory_mb,
                "total_gpu_count": self.total_gpu_count,
                "scheduler_metadata": self.scheduler_metadata,
                "anomalies": ([anomaly.model_dump_json() for anomaly in self.anomalies] if self.anomalies else None),
                "checkpoints": (
                    [checkpoint.model_dump_json() for checkpoint in self.checkpoints] if self.checkpoints else None
                ),
                "aps_spec": self.aps_spec.model_dump_json() if self.aps_spec else None,
                "config_validation_failure_types": (
                    [failure_type.value for failure_type in self.config_validation_failure_types]
                    if self.config_validation_failure_types
                    else None
                ),
                "last_state_transition_time": (
                    self.last_state_transition_time.isoformat() if self.last_state_transition_time else None
                ),
                "last_updated_at": (self.last_updated_at.isoformat() if self.last_updated_at else None),
                "attribution_request_emitted": self.attribution_request_emitted,
                "retry_of": self.retry_of,
                "log_file_paths": self.log_file_paths,
                "worker_cluster_name": self.worker_cluster_name,
            }
        )

    @classmethod
    def get_time_attribute(cls, filter_target: JobTimeFilterTarget) -> Any:
        match filter_target:
            case JobTimeFilterTarget.SUBMIT_TIME:
                return cls.submit_time
            case JobTimeFilterTarget.START_TIME:
                return cls.start_time
            case JobTimeFilterTarget.END_TIME:
                return cls.end_time
            case JobTimeFilterTarget.LAST_UPATED_TIME:
                return cls.last_updated_at

        raise ValueError(f"Unsupported time filter target: {filter_target}")


def _get_user_from_squeue(json: dict[str, Any]):
    """
    Extract the user from the json dictionary returned by Slurm squeue.
    Slurm stores user ids internally, it's only the client tools such as squeue
    that convert a user id to a user name. However, if these tools run in a
    containerized environment, the user id to user name mapping may not be available.
    In this case, the user_name field will be empty, and we rely on the mail_user
    which is instead resolved by Slurm when the batch starts. The mail_user can
    be set to any email in the sbatch script, but if this is not done, then the
    default value is the submitting user. As a last resort, should the mail user
    also be empty, we will user the user id, which should always be there.
    The mail_user field falls back through submitting user, then user id.
    """
    user = json["user_name"]
    if not user:
        user = json["mail_user"]
    if not user:
        user = "user_" + json["user_id"]
    return user


class RecoverySequenceRecord(DataClassBase):
    """Durable identity and canonical W3C parent for one recovery operation."""

    __tablename__ = "recovery_sequences"

    sequence_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    scheduler_system: Mapped[str] = mapped_column(String(32), kw_only=True)
    scheduler_scope: Mapped[str] = mapped_column(String(256), kw_only=True)
    job_id: Mapped[str] = mapped_column(String(256), kw_only=True)
    source_attempt_index: Mapped[int] = mapped_column(Integer, kw_only=True)
    operation: Mapped[str] = mapped_column(String(32), kw_only=True)
    traceparent: Mapped[str] = mapped_column(String(128), kw_only=True)
    tracestate: Mapped[str | None] = mapped_column(String(512), default=None, kw_only=True)
    trigger_type: Mapped[str] = mapped_column(String(32), kw_only=True)
    trigger_identifier: Mapped[str | None] = mapped_column(String(256), default=None, kw_only=True)
    triggered_at: Mapped[datetime] = mapped_column(TZDateTimeMicrosecondsType, kw_only=True)
    status: Mapped[str] = mapped_column(String(32), default="active", kw_only=True)
    completed_at: Mapped[datetime | None] = mapped_column(TZDateTimeMicrosecondsType, default=None, kw_only=True)
    created_at: Mapped[datetime] = mapped_column(TZDateTimeMicrosecondsType, kw_only=True)
    updated_at: Mapped[datetime] = mapped_column(TZDateTimeMicrosecondsType, kw_only=True)

    __table_args__ = (
        UniqueConstraint(
            "scheduler_system",
            "scheduler_scope",
            "job_id",
            "source_attempt_index",
            "operation",
            name="uq_recovery_sequence_natural_key",
        ),
        Index("idx_recovery_sequence_status_triggered", "status", "triggered_at"),
        Index("idx_recovery_sequence_job_attempt", "job_id", "source_attempt_index"),
    )


class ApplicationIntentionRecord(DataClassBase):
    """Persistent job-level instruction supplied by the running application.

    The record intentionally has no foreign key to an execution attempt. An
    application can report an unrecoverable condition before SAC has published
    the first attempt, and the instruction must also apply to retry attempts
    created after the original request.
    """

    __tablename__ = "application_intentions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, init=False, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(64), kw_only=True)
    desired_state: Mapped[str] = mapped_column(String(32), kw_only=True)
    reason: Mapped[str | None] = mapped_column(Text, default=None, kw_only=True)
    audit_attempt_index: Mapped[int | None] = mapped_column(Integer, default=None, kw_only=True)
    created_at: Mapped[datetime] = mapped_column(TZDateTimeMicrosecondsType, kw_only=True)

    __table_args__ = (
        UniqueConstraint("job_id", name="uq_application_intention_job_id"),
        Index("idx_application_intention_created_at", "created_at"),
    )


class RecoveryMilestoneRecord(DataClassBase):
    """One idempotently reported scheduler-action completion occurrence."""

    __tablename__ = "recovery_milestones"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, init=False, autoincrement=True)
    sequence_id: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        ForeignKey("recovery_sequences.sequence_id", ondelete="CASCADE"),
        kw_only=True,
    )
    idempotency_key: Mapped[str] = mapped_column(String(128), kw_only=True)
    operation: Mapped[str] = mapped_column(String(32), kw_only=True)
    action: Mapped[str] = mapped_column(String(32), kw_only=True)
    outcome: Mapped[str] = mapped_column(String(32), kw_only=True)
    completes_sequence: Mapped[bool] = mapped_column(Boolean, kw_only=True)
    completed_at: Mapped[datetime] = mapped_column(TZDateTimeMicrosecondsType, kw_only=True)
    created_at: Mapped[datetime] = mapped_column(TZDateTimeMicrosecondsType, kw_only=True)

    __table_args__ = (
        UniqueConstraint("sequence_id", "idempotency_key", name="uq_recovery_milestone_occurrence"),
        Index("idx_recovery_milestone_sequence_completed", "sequence_id", "completed_at"),
    )


class AdviceReconciliationStatusRecord(DataClassBase):
    """One durable reconciliation observation for an Advice generation."""

    __tablename__ = "advice_reconciliation_status"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, init=False, autoincrement=True)
    advice_id: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        ForeignKey("advice.heimdall_id", ondelete="CASCADE"),
        kw_only=True,
    )
    observed_generation: Mapped[int] = mapped_column(Integer, kw_only=True)
    disposition: Mapped[str] = mapped_column(String(32), kw_only=True)
    reason: Mapped[str] = mapped_column(String(64), kw_only=True)
    observed_at: Mapped[datetime] = mapped_column(TZDateTimeMicrosecondsType, kw_only=True)
    transitioned_at: Mapped[datetime] = mapped_column(TZDateTimeMicrosecondsType, kw_only=True)
    diagnostic_message: Mapped[str | None] = mapped_column(Text, default=None, kw_only=True)

    __table_args__ = (
        UniqueConstraint(
            "advice_id",
            "observed_generation",
            name="uq_advice_status_generation",
        ),
    )


class AdviceRecord(DataClassBase):
    """
    Storage model for Advice records.

    Represents advice computed by the Advisor for a job, including desired state,
    node constraints, and detected issues. This table is managed by internal services
    and exposed via job_manager HTTP API.

    The nested AdviceRecommendation is flattened: advice_state and node constraints
    are stored as separate columns for query efficiency.
    """

    __tablename__ = "advice"

    # Primary key - UUID assigned by Heimdall
    heimdall_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)

    # Visibility flag - allows soft deletes and gradual construction of advice
    # Default False: advice is hidden until explicitly made visible
    visible: Mapped[bool] = mapped_column(Boolean, default=False, kw_only=True)

    # Scheduler-specific job identifier
    job_id: Mapped[str] = mapped_column(String(256), kw_only=True)

    # Scheduler metadata (e.g., CRD kind, namespace, labels)
    scheduler_metadata: Mapped[dict | None] = mapped_column(JSON, default=None, kw_only=True)

    # Restart/attempt count for this job
    attempt_index: Mapped[int] = mapped_column(Integer, kw_only=True)

    # Flattened AdviceRecommendation fields
    # State: "Restarted" or "Terminated" (matches AdviceState enum values)
    advice_state: Mapped[str | None] = mapped_column(String(32), default=None, kw_only=True)

    # Node constraints from AdviceRecommendation.node_constraints
    node_exclusions: Mapped[list[str] | None] = mapped_column(JSON, default=None, kw_only=True)
    node_inclusions: Mapped[list[str] | None] = mapped_column(JSON, default=None, kw_only=True)

    # Issues detected for this job (stored as JSON array of Issue objects)
    issues: Mapped[list[Issue] | None] = mapped_column(IssuesType, default=None, kw_only=True)

    # FACT attribution data from ADI (stored as JSON dict with node-level and global attribution details)
    fact_attribution: Mapped[dict | None] = mapped_column(JSON, default=None, kw_only=True)

    # Durable, per-item recovery context. All fields are nullable so Advice
    # written before recovery tracing remains readable and actionable.
    recovery_sequence_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid,
        ForeignKey("recovery_sequences.sequence_id", ondelete="SET NULL"),
        default=None,
        kw_only=True,
    )
    recovery_traceparent: Mapped[str | None] = mapped_column(String(128), default=None, kw_only=True)
    recovery_tracestate: Mapped[str | None] = mapped_column(String(512), default=None, kw_only=True)
    recovery_triggered_at: Mapped[datetime | None] = mapped_column(
        TZDateTimeMicrosecondsType,
        default=None,
        kw_only=True,
    )
    recovery_operation: Mapped[str | None] = mapped_column(String(32), default=None, kw_only=True)

    # Desired-state lifecycle. Generation changes only when scheduler-affecting
    # intent changes.
    generation: Mapped[int] = mapped_column(Integer, default=1, kw_only=True)
    desired_state_changed_at: Mapped[datetime] = mapped_column(
        TZDateTimeMicrosecondsType,
        default_factory=lambda: datetime.now(UTC),
        kw_only=True,
    )

    reconciliation_statuses: Mapped[list[AdviceReconciliationStatusRecord]] = relationship(
        order_by=AdviceReconciliationStatusRecord.transitioned_at,
        cascade="all, delete-orphan",
        default_factory=list,
        init=False,
        lazy="selectin",
    )

    # Timestamps (all UTC)
    created_at: Mapped[datetime] = mapped_column(TZDateTimeType, kw_only=True)
    updated_at: Mapped[datetime] = mapped_column(TZDateTimeType, kw_only=True)

    __table_args__ = (
        Index("idx_advice_job_id", "job_id"),
        Index("idx_advice_created_at", "created_at"),
        Index("idx_advice_visible", "visible"),
        Index("idx_advice_recovery_sequence", "recovery_sequence_id"),
        UniqueConstraint("job_id", "attempt_index", name="uq_advice_job_attempt"),
        ForeignKeyConstraint(
            ["heimdall_id"],
            ["job_execution_records.heimdall_job_id"],
            name="fk_advice_heimdall_job_id",
        ),
    )


class DagDependency(DataClassBase):
    """
    ORM model for DAG dependency records.

    Each row represents a dependency relationship between a dependent (child) job and a dependency (parent) job.
    When the parent job satisfies the trigger condition, the dependent job becomes eligible for the specified action.

    Attributes:
        dependent_job_id: The Slurm job ID of the child job that depends on another job.
        dependent_job_action: The action to take on the child job when all dependencies are met (e.g., "release").
        dependency_job_identifier: The identifier of the parent job — either a Slurm job ID or job name.
        dependency_job_identifier_type: The type of identifier — "id" or "name".
        dependency_trigger_condition: The condition that must be met (e.g., "completed").
        action_status: The current status of the dependency action lifecycle.
        action_time: The timestamp when the action was last attempted.
        reason_code: Stable machine-readable explanation for the latest evaluation.
        reason_message: Human-readable explanation for the latest evaluation.
        reason_details: Structured context for the latest evaluation.
        last_evaluated_at: The timestamp of the latest dependency evaluation.
    """

    __tablename__ = "dag_dependency"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, init=False, autoincrement=True)
    dependent_job_id: Mapped[str] = mapped_column(String(64), kw_only=True)
    dependent_job_action: Mapped[str] = mapped_column(String(32), kw_only=True)
    dependency_job_identifier: Mapped[str] = mapped_column(String(256), kw_only=True)
    dependency_job_identifier_type: Mapped[str] = mapped_column(String(16), kw_only=True)
    dependency_trigger_condition: Mapped[str] = mapped_column(String(32), kw_only=True)
    action_status: Mapped[str] = mapped_column(
        String(32), default=DagDependencyActionStatus.NOT_ELIGIBLE.value, kw_only=True
    )
    action_time: Mapped[datetime | None] = mapped_column(TZDateTimeType, default=None, kw_only=True)
    reason_code: Mapped[str | None] = mapped_column(String(64), default=None, kw_only=True)
    reason_message: Mapped[str | None] = mapped_column(Text, default=None, kw_only=True)
    reason_details: Mapped[dict[str, Any] | None] = mapped_column(JSON, default=None, kw_only=True)
    last_evaluated_at: Mapped[datetime | None] = mapped_column(TZDateTimeType, default=None, kw_only=True)

    __table_args__ = (
        Index("idx_dag_dep_dependent_job_id", "dependent_job_id"),
        Index("idx_dag_dep_identifier", "dependency_job_identifier"),
        Index("idx_dag_dep_action_status", "action_status"),
        UniqueConstraint(
            "dependent_job_id",
            "dependency_job_identifier",
            "dependency_job_identifier_type",
            name="uq_dag_dep_job_identifier",
        ),
    )
