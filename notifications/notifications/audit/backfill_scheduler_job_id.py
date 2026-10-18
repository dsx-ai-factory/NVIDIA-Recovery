# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Bounded, resumable projection backfill; run after the additive schema migration."""

import argparse
import json
import uuid
from dataclasses import asdict, dataclass

from sqlmodel import Session, col, select

from notifications.audit.identity import scheduler_job_id_from_payload
from notifications.database import get_engine
from notifications.models.audit_events import AuditEventOrm


@dataclass(frozen=True)
class BackfillResult:
    """Committed batch progress; retain through_id and next_after_id to resume."""

    scanned: int
    """Number of rows inspected, including previously projected rows."""
    updated: int
    """Number of valid scheduler identities projected."""
    unmappable_ids: list[uuid.UUID]
    """Rows left unchanged and still readable by their exact attempt identity."""
    through_id: uuid.UUID | None
    """Inclusive upper bound fixed for this pass."""
    next_after_id: uuid.UUID | None
    """Resume strictly after this ID, or finish the pass when absent."""


def backfill_scheduler_job_ids(
    session: Session,
    *,
    batch_size: int = 500,
    after_id: uuid.UUID | None = None,
    through_id: uuid.UUID | None = None,
) -> BackfillResult:
    """Commit at most 1000 projections without modifying any event or payload.

    IDs, not producer timestamps, bound the scan. A final pass from the start
    after old writers have drained catches rows committed behind a prior cursor.
    Null projections are the only candidates, so replaying a batch is harmless.
    """
    if not 1 <= batch_size <= 1000:
        raise ValueError("batch_size must be between 1 and 1000")
    if through_id is None:
        through_id = session.exec(select(AuditEventOrm.id).order_by(col(AuditEventOrm.id).desc()).limit(1)).first()
    if through_id is None:
        return BackfillResult(0, 0, [], None, None)

    statement = select(AuditEventOrm).where(col(AuditEventOrm.id) <= through_id)
    if after_id is not None:
        statement = statement.where(col(AuditEventOrm.id) > after_id)
    rows = list(session.exec(statement.order_by(col(AuditEventOrm.id)).limit(batch_size + 1)))
    batch = rows[:batch_size]
    unmappable_ids = []
    updated = 0
    for row in batch:
        if row.scheduler_job_id is not None:
            continue
        job_id = scheduler_job_id_from_payload(row.heimdall_job_id, row.payload)
        if job_id is None:
            unmappable_ids.append(row.id)
        else:
            row.scheduler_job_id = job_id
            updated += 1
    next_after_id = batch[-1].id if len(rows) > batch_size else None
    session.commit()
    return BackfillResult(len(batch), updated, unmappable_ids, through_id, next_after_id)


def main() -> None:
    """Backfill one batch using the service's existing database and TLS settings."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--after-id", type=uuid.UUID)
    parser.add_argument("--through-id", type=uuid.UUID)
    args = parser.parse_args()
    if not 1 <= args.batch_size <= 1000:
        parser.error("--batch-size must be between 1 and 1000")
    if args.after_id is not None and args.through_id is None:
        parser.error("--after-id requires the --through-id from the previous batch")
    with Session(get_engine()) as session:
        result = backfill_scheduler_job_ids(
            session, batch_size=args.batch_size, after_id=args.after_id, through_id=args.through_id
        )
    print(json.dumps(asdict(result), default=str))


if __name__ == "__main__":
    main()
