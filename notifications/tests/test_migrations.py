# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

from notifications import database

ALEMBIC_ROOT = Path(__file__).parent.parent / "notifications/_migrations"
VERSIONS_ROOT = ALEMBIC_ROOT / "versions"
BASELINE_REVISION = "000000000002"
AUDIT_EVENTS_REVISION = "000000000003"
AUDIT_JOB_REVISION = "000000000004"


def test_notifications_history_is_linear_from_the_parentless_baseline() -> None:
    config = Config()
    config.set_main_option("script_location", str(ALEMBIC_ROOT))
    scripts = ScriptDirectory.from_config(config)
    revisions = list(scripts.walk_revisions())

    assert [revision.revision for revision in revisions] == [
        AUDIT_JOB_REVISION,
        AUDIT_EVENTS_REVISION,
        BASELINE_REVISION,
    ]
    assert revisions[0].down_revision == AUDIT_EVENTS_REVISION
    assert revisions[1].down_revision == BASELINE_REVISION
    assert revisions[2].down_revision is None
    assert scripts.get_bases() == [BASELINE_REVISION]
    assert scripts.get_heads() == [AUDIT_JOB_REVISION]
    assert set(VERSIONS_ROOT.glob("*.py")) == {
        VERSIONS_ROOT / "000000000002_heimdall_oss_notifications_baseline.py",
        VERSIONS_ROOT / "000000000003_add_audit_events.py",
        VERSIONS_ROOT / "000000000004_add_audit_scheduler_job.py",
    }


def test_startup_migrations_run_alembic_upgrade(mocker) -> None:
    upgrade = mocker.patch("notifications.database.alembic.command.upgrade")
    inspector = mocker.patch("notifications.database.sa_inspect")
    inspector.return_value.get_table_names.return_value = []
    mocker.patch("notifications.database.get_engine")

    database.run_migrations()

    upgrade.assert_called_once()
    assert upgrade.call_args.args[1] == "head"


@pytest.mark.anyio
async def test_lifespan_invokes_migrations(mocker) -> None:
    run_migrations = mocker.patch("notifications.database.run_migrations")

    async with database.lifespan(mocker.MagicMock()):
        pass

    run_migrations.assert_called_once_with()


def test_migrations_reject_unversioned_managed_tables(mocker) -> None:
    upgrade = mocker.patch("notifications.database.alembic.command.upgrade")
    inspector = mocker.patch("notifications.database.sa_inspect")
    inspector.return_value.get_table_names.return_value = ["Notifications"]
    mocker.patch("notifications.database.get_engine")

    with pytest.raises(RuntimeError, match="fresh empty database/schema"):
        database.run_migrations()

    upgrade.assert_not_called()


def test_migrations_reject_managed_tables_with_empty_version_table(mocker) -> None:
    upgrade = mocker.patch("notifications.database.alembic.command.upgrade")
    inspector = mocker.patch("notifications.database.sa_inspect")
    inspector.return_value.get_table_names.return_value = [database.VERSION_TABLE, "UserPreferences"]
    engine = mocker.patch("notifications.database.get_engine").return_value
    engine.connect.return_value.__enter__.return_value.execute.return_value.scalar_one_or_none.return_value = None

    with pytest.raises(RuntimeError, match="has no revision"):
        database.run_migrations()

    upgrade.assert_not_called()


def test_migrations_wrap_unknown_revision_as_fresh_database_error(mocker) -> None:
    upgrade = mocker.patch("notifications.database.alembic.command.upgrade")
    upgrade.side_effect = Exception("Can't locate revision identified by 'legacy_revision'")
    inspector = mocker.patch("notifications.database.sa_inspect")
    inspector.return_value.get_table_names.return_value = [database.VERSION_TABLE, "Notifications"]
    mocker.patch("notifications.database.get_engine")

    with pytest.raises(RuntimeError, match="fresh empty database/schema"):
        database.run_migrations()
