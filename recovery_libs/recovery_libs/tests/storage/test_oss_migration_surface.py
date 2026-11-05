# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory

PACKAGE_ROOT = Path(__file__).resolve().parents[2]
ALEMBIC_ROOT = PACKAGE_ROOT / "storage" / "alembic"
VERSIONS_ROOT = ALEMBIC_ROOT / "versions"
BASELINE_REVISION = "000000000001"
DAG_OBSERVABILITY_REVISION = "000000000002"
DURABLE_RECOVERY_REVISION = "000000000003"
ADVICE_LIFECYCLE_REVISION = "000000000004"
ON_DEMAND_REMOVAL_REVISION = "000000000005"
HEAD_REVISION = "000000000006"


def test_storage_history_is_linear_from_parentless_baseline() -> None:
    config = Config()
    config.set_main_option("script_location", str(ALEMBIC_ROOT))
    scripts = ScriptDirectory.from_config(config)
    revisions = list(scripts.walk_revisions())

    assert [revision.revision for revision in revisions] == [
        HEAD_REVISION,
        ON_DEMAND_REMOVAL_REVISION,
        ADVICE_LIFECYCLE_REVISION,
        DURABLE_RECOVERY_REVISION,
        DAG_OBSERVABILITY_REVISION,
        BASELINE_REVISION,
    ]
    assert revisions[-1].down_revision is None
    assert revisions[0].down_revision == ON_DEMAND_REMOVAL_REVISION
    assert revisions[1].down_revision == ADVICE_LIFECYCLE_REVISION
    assert revisions[2].down_revision == DURABLE_RECOVERY_REVISION
    assert revisions[3].down_revision == DAG_OBSERVABILITY_REVISION
    assert revisions[4].down_revision == BASELINE_REVISION
    assert scripts.get_bases() == [BASELINE_REVISION]
    assert scripts.get_heads() == [HEAD_REVISION]
    assert sorted(path.name for path in VERSIONS_ROOT.glob("*.py")) == [
        "000000000001_heimdall_oss_storage_baseline.py",
        "000000000002_add_dag_dependency_observability.py",
        "000000000003_durable_recovery_tracing.py",
        "000000000004_actionable_advice_lifecycle.py",
        "000000000005_remove_on_demand_job_updates.py",
        "000000000006_add_application_intentions.py",
    ]
