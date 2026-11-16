"""Slurm identity validation at backend compatibility boundaries."""

from collections.abc import Mapping

SLURM_IDENTITY_KEYS = (
    "slurm_job_id",
    "slurm_raw_job_id",
    "slurm_array_job_id",
    "slurm_array_task_id",
)
"""Scheduler metadata keys that must survive partial job observations."""


def _decimal(value: object, *, positive: bool) -> str:
    if not isinstance(value, str) or not value.isascii() or not value.isdecimal():
        raise ValueError("Slurm identity must be a decimal string")
    number = int(value)
    if str(number) != value or number < int(positive) or number >= 4294967294:
        raise ValueError("Slurm identity is outside the concrete ID range")
    return value


def project_slurm_job_group(job_id: str, metadata: Mapping[str, object], job_group_id: str | None) -> str | None:
    """Validate supplied Slurm identity and project an explicit array ID to its group.

    Missing metadata is permitted for partial observations. Each supplied array
    component must agree with the canonical job ID, including task zero.
    """
    if metadata.get("scheduler") != "slurm":
        return job_group_id
    if "slurm_job_id" in metadata and metadata["slurm_job_id"] != job_id:
        raise ValueError("Slurm job ID conflicts with job_id")
    if "slurm_raw_job_id" in metadata:
        _decimal(metadata["slurm_raw_job_id"], positive=True)
    array_keys = ("slurm_array_job_id", "slurm_array_task_id")
    if "_" not in job_id and not any(key in metadata for key in array_keys):
        return job_group_id
    parts = job_id.split("_")
    if len(parts) != 2:
        raise ValueError("Slurm array metadata requires a canonical task job_id")
    array_id = _decimal(parts[0], positive=True)
    task_id = _decimal(parts[1], positive=False)
    for key, expected in zip(array_keys, (array_id, task_id), strict=True):
        if key in metadata and metadata[key] != expected:
            raise ValueError("Slurm array metadata conflicts with job_id")
    if job_group_id is not None and job_group_id != array_id:
        raise ValueError("Slurm array ID conflicts with job_group_id")
    return array_id if "slurm_array_job_id" in metadata else job_group_id
