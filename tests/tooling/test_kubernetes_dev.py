# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exercise bootstrap recovery through the developer entry point without a cluster."""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parents[2]


@pytest.fixture
def profile(tmp_path):
    """Provide controllable CLI responses while running the real development script."""
    script = tmp_path / "deploy/tilt/kubernetes/dev.sh"
    script.parent.mkdir(parents=True)
    shutil.copyfile(REPO_ROOT / "deploy/tilt/kubernetes/dev.sh", script)
    commands = tmp_path / "bin"
    commands.mkdir()
    fake = commands / "client"
    fake.write_text(
        f"#!{sys.executable}\n"
        """import os
import sys
import time
from pathlib import Path

root = Path(os.environ["FAKE_CLUSTER_DIR"])
cluster = root / "cluster"
command = Path(sys.argv[0]).name
args = sys.argv[1:]
with (root / "calls").open("a") as log:
    log.write(command + " " + " ".join(args) + "\\n")
if command == "kind":
    if args[:2] == ["get", "clusters"]:
        if cluster.exists():
            print(os.environ["KUBERNETES_DEV_CLUSTER"])
    elif args[:2] == ["create", "cluster"]:
        cluster.touch()
        Path(os.environ["KUBECONFIG"]).write_text("test config")
        sys.exit(int(os.environ.get("CREATE_STATUS", "0")))
    elif args[:2] == ["delete", "cluster"]:
        status = int(os.environ.get("DELETE_STATUS", "0"))
        if status == 0:
            cluster.unlink(missing_ok=True)
        sys.exit(status)
elif command == "uv":
    (root / "fixture-url").write_text(os.environ["ADVICE_FIXTURE_URL"])
    sys.exit(int(os.environ.get("TEST_STATUS", "0")))
elif command == "kubectl":
    if "port-forward" in args:
        (root / "forward-pid").write_text(str(os.getpid()))
        print("Forwarding from 127.0.0.1:32123 -> 8081", flush=True)
        time.sleep(60)
    if cluster.exists():
        print(os.environ.get("CLUSTER_UID", "original-uid"), end="")
    sys.exit(int(os.environ.get("UID_STATUS", "0")))
"""
    )
    fake.chmod(0o755)
    for name in ("kind", "kubectl", "tilt", "uv"):
        (commands / name).symlink_to(fake)
    state = tmp_path / "deploy/tilt/tmp/test-cluster"
    env = {
        **os.environ,
        "PATH": f"{commands}{os.pathsep}{os.environ['PATH']}",
        "FAKE_CLUSTER_DIR": str(tmp_path),
        "KUBERNETES_DEV_CLUSTER": "test-cluster",
    }

    def run(action, **overrides):
        return subprocess.run(
            ["bash", str(script), action],
            env={**env, **overrides},
            capture_output=True,
            text=True,
            check=False,
            timeout=15,
        )

    return run, state, tmp_path


@pytest.mark.parametrize("action", ["up", "down"])
def test_partial_bootstrap_retains_identity_for_retry_or_teardown(profile, action):
    """Readiness failure remains a failure but does not strand an identifiable cluster."""
    run, state, root = profile
    result = run("up", CREATE_STATUS="7")
    assert result.returncode == 7
    assert (state / "owned").read_text().strip() == "original-uid"
    assert "tilt " not in (root / "calls").read_text()
    assert run(action).returncode == 0
    assert (root / "cluster").exists() == (action == "up")
    assert (root / "calls").read_text().count("kind create cluster") == 1


@pytest.mark.parametrize("uid_status,uid", [("1", ""), ("0", "")])
@pytest.mark.parametrize("create_status", ["0", "7"])
def test_unidentifiable_bootstrap_is_cleaned_and_can_retry(profile, uid_status, uid, create_status):
    """A failed or empty UID lookup cannot leave an unusable ownership marker."""
    run, state, root = profile
    result = run("up", CREATE_STATUS=create_status, UID_STATUS=uid_status, CLUSTER_UID=uid)
    assert result.returncode == (int(create_status) or 1)
    assert not (root / "cluster").exists()
    assert not (state / "owned").exists()
    assert not (state / "kubeconfig").exists()
    assert "tilt " not in (root / "calls").read_text()
    assert run("up").returncode == 0


def test_failed_cleanup_preserves_config_for_manual_recovery(profile):
    """Cleanup failure preserves access information without claiming cluster ownership."""
    run, state, root = profile
    result = run("up", CREATE_STATUS="7", UID_STATUS="1", CLUSTER_UID="", DELETE_STATUS="9")
    assert result.returncode == 7
    assert (root / "cluster").exists()
    assert (state / "kubeconfig").is_file()
    assert not (state / "owned").exists()
    assert run("down").returncode != 0
    assert (root / "calls").read_text().count("kind delete cluster") == 1


@pytest.mark.parametrize("action", ["up", "down"])
@pytest.mark.parametrize("ownership", [None, "", "old-uid"])
def test_existing_unowned_or_replaced_cluster_is_never_deleted(profile, action, ownership):
    """Bootstrap recovery does not bypass the identity gate for an existing cluster."""
    run, state, root = profile
    (root / "cluster").touch()
    state.mkdir(parents=True)
    (state / "kubeconfig").write_text("test config")
    if ownership is not None:
        (state / "owned").write_text(ownership)
    assert run(action).returncode != 0
    assert (root / "cluster").exists()
    calls = (root / "calls").read_text() if (root / "calls").exists() else ""
    assert "kind delete" not in calls
    assert "kind create" not in calls
    assert "tilt " not in calls


@pytest.mark.parametrize("test_status", ["0", "7"])
def test_fixture_forward_is_scoped_to_test_process_and_preserves_exit_status(profile, test_status):
    """Tests receive their own local endpoint and reap the forward on success or failure."""
    run, _, root = profile
    assert run("up").returncode == 0
    result = run("test", TEST_STATUS=test_status)
    assert result.returncode == int(test_status)
    assert (root / "fixture-url").read_text() == "http://127.0.0.1:32123"
    with pytest.raises(ProcessLookupError):
        os.kill(int((root / "forward-pid").read_text()), 0)
    calls = (root / "calls").read_text()
    assert "--context kind-test-cluster -n recovery-dev port-forward --address 127.0.0.1" in calls
