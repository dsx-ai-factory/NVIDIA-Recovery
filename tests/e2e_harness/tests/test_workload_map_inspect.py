# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Prove inspection is read-only and cannot turn missing evidence into qualification."""

import argparse
import json
import subprocess
from types import SimpleNamespace

import pytest
from e2e_harness import workload_map_inspect as inspection


@pytest.fixture
def setup(tmp_path, monkeypatch):
    probe = tmp_path / "probe"
    probe.touch()
    kubeconfig = tmp_path / "config"
    kubeconfig.touch()
    args = argparse.Namespace(
        probe=probe,
        kubeconfig=kubeconfig,
        context="test-context",
        namespace="workloads",
        resource="jobset",
        name="training",
        controller_namespace=None,
        controller_deployment=None,
    )
    workload = {
        "apiVersion": "jobset.x-k8s.io/v1alpha2",
        "kind": "JobSet",
        "metadata": {"name": "training", "namespace": "workloads", "uid": "workload-uid", "resourceVersion": "3"},
        "spec": {"environment": {"SECRET": "private-value"}},
    }
    sdk = {
        "sdk_version": "v0.3.0",
        "definition_name": "jobset-definition",
        "definition_sha256": "definition-digest",
        "statuses": ["Initializing", "Running"],
        "components": {"workers": {"": {"replicas": 6}}},
        "suspend_supported": True,
        "capability_errors": {},
    }
    state = SimpleNamespace(args=args, workload=workload, sdk=sdk, calls=[], failures={}, replies={})

    def run(command, **kwargs):
        state.calls.append((command, kwargs))
        if command[0] == str(probe):
            key = "probe"
            reply = state.sdk
        elif "version" in command:
            key = "version"
            reply = {"serverVersion": {"gitVersion": "v1.35.0", "platform": "linux/arm64"}}
        else:
            key = command[command.index("get") + 1]
            reply = (
                state.workload
                if key in {"jobsets.jobset.x-k8s.io", "pytorchjobs.kubeflow.org"}
                else state.replies.get(key)
            )
        if key in state.failures:
            failure = state.failures[key]
            if isinstance(failure, BaseException):
                raise failure
            return SimpleNamespace(returncode=1, stdout="", stderr=failure)
        return SimpleNamespace(returncode=0, stdout=json.dumps(reply) if reply is not None else "", stderr="")

    monkeypatch.setattr(inspection.subprocess, "run", run)
    return state


def test_reads_explicit_context_without_claiming_lifecycle_qualification(setup):
    report = inspection.inspect_cluster(setup.args)
    assert report["mode"] == "read_only"
    assert report["input_identity"]["uid"] == "workload-uid"
    assert report["workload_map"]["statuses"] == ["Initializing", "Running"]
    assert report["checks"]["sdk_extraction"]["outcome"] == "pass"
    for capability in ("lifecycle", "instrumentation", "pod_association", "failure_recovery", "controller"):
        assert report["checks"][capability]["outcome"] == "not_tested"
    assert "private-value" not in json.dumps(report)
    for command, kwargs in setup.calls:
        assert kwargs["timeout"] <= 35
        if command[0] == "kubectl":
            assert command[command.index("--kubeconfig") + 1] == str(setup.args.kubeconfig)
            assert command[command.index("--context") + 1] == "test-context"
            positional = []
            arguments = iter(command[1:])
            for argument in arguments:
                if argument in {"--kubeconfig", "--context", "--namespace", "-o"}:
                    next(arguments)
                elif not argument.startswith("--"):
                    positional.append(argument)
            assert positional[0] in {"get", "version"}
            assert kwargs["input"] is None
        else:
            assert command == [str(setup.args.probe), "--operation", "inspect"]
            assert json.loads(kwargs["input"]) == setup.workload


def test_missing_workload_is_not_a_pass_and_does_not_invoke_probe(setup):
    setup.workload = None
    report = inspection.inspect_cluster(setup.args)
    assert report["checks"]["workload_access"]["outcome"] == "unsupported"
    assert report["checks"]["sdk_extraction"]["outcome"] == "not_tested"
    assert all(command[0] == "kubectl" for command, _ in setup.calls)


@pytest.mark.parametrize("failure", ["Forbidden: private-value", subprocess.TimeoutExpired("kubectl", 35)])
def test_access_failure_is_inconclusive_and_does_not_leak_manifest_errors(setup, failure):
    setup.failures["jobsets.jobset.x-k8s.io"] = failure
    report = inspection.inspect_cluster(setup.args)
    assert report["checks"]["workload_access"]["outcome"] == "inconclusive"
    assert report["checks"]["sdk_extraction"]["outcome"] == "not_tested"
    assert "private-value" not in json.dumps(report)


def test_failed_capability_retains_successful_observations(setup):
    setup.sdk["capability_errors"] = {"components.workers.scale": "cannot multiply number and null"}
    report = inspection.inspect_cluster(setup.args)
    assert report["checks"]["sdk_extraction"]["outcome"] == "fail"
    assert report["checks"]["sdk:components.workers.scale"]["outcome"] == "fail"
    assert report["workload_map"]["statuses"] == ["Initializing", "Running"]


def test_probe_failure_and_missing_mapping_are_not_success(setup):
    setup.failures["probe"] = "private-value"
    report = inspection.inspect_cluster(setup.args)
    assert report["checks"]["sdk_extraction"]["outcome"] == "inconclusive"
    setup.failures.clear()
    setup.sdk["suspend_supported"] = False
    report = inspection.inspect_cluster(setup.args)
    assert report["checks"]["suspend_mapping"]["outcome"] == "unsupported"
    assert report["checks"]["lifecycle"]["outcome"] == "not_tested"


def test_controller_digests_follow_uid_ownership_and_rbac_failure_is_retained(setup):
    setup.args.controller_namespace = "operators"
    setup.args.controller_deployment = "controller"
    setup.replies["deployments.apps"] = {
        "metadata": {"uid": "deployment-uid", "generation": 2},
        "spec": {"replicas": 1, "template": {"spec": {"containers": [{"image": "controller:v1"}]}}},
        "status": {"observedGeneration": 2, "replicas": 1, "updatedReplicas": 1, "availableReplicas": 1},
    }
    setup.replies["replicasets.apps"] = {
        "items": [
            {
                "metadata": {
                    "uid": "rs-uid",
                    "ownerReferences": [{"uid": "deployment-uid", "controller": True}],
                }
            }
        ]
    }
    setup.replies["pods"] = {
        "items": [
            {
                "metadata": {"ownerReferences": [{"uid": owner, "controller": True}]},
                "status": {"containerStatuses": [{"imageID": digest}]},
            }
            for owner, digest in (("rs-uid", "sha256:owned"), ("other-rs", "sha256:foreign"))
        ]
    }
    report = inspection.inspect_cluster(setup.args)
    assert report["controller"]["image_ids"] == ["sha256:owned"]
    assert report["checks"]["controller"]["outcome"] == "pass"
    setup.failures["pods"] = "Forbidden"
    report = inspection.inspect_cluster(setup.args)
    assert report["checks"]["controller_provenance"]["outcome"] == "inconclusive"
    assert report["checks"]["sdk_extraction"]["outcome"] == "pass"


def test_cli_requires_explicit_selection_and_reports_nonzero_on_failed_check(setup, capsys):
    with pytest.raises(SystemExit) as error:
        inspection.main([])
    assert error.value.code == 2
    argv = [
        "--probe",
        str(setup.args.probe),
        "--kubeconfig",
        str(setup.args.kubeconfig),
        "--context",
        "test-context",
        "--namespace",
        "workloads",
        "--resource",
        "jobset",
        "--name",
        "training",
    ]
    assert inspection.main(argv) == 0
    capsys.readouterr()
    setup.failures["version"] = "Forbidden"
    assert inspection.main(argv) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["checks"]["cluster_access"]["outcome"] == "inconclusive"
    with pytest.raises(SystemExit) as error:
        inspection.main([*argv, "--controller-deployment", "controller"])
    assert error.value.code == 2


def test_pytorch_selection_uses_qualified_resource(setup):
    setup.args.resource = "pytorchjob"
    setup.workload["apiVersion"] = "kubeflow.org/v1"
    setup.workload["kind"] = "PyTorchJob"
    report = inspection.inspect_cluster(setup.args)
    assert report["input_identity"]["kind"] == "PyTorchJob"
    assert any("pytorchjobs.kubeflow.org" in command for command, _ in setup.calls)
    assert report["checks"]["lifecycle"]["outcome"] == "not_tested"


@pytest.mark.parametrize("missing", ["--kubeconfig", "--context", "--namespace", "--resource", "--name"])
def test_no_ambient_selection_fallback(setup, missing):
    pairs = {
        "--probe": str(setup.args.probe),
        "--kubeconfig": str(setup.args.kubeconfig),
        "--context": "test-context",
        "--namespace": "workloads",
        "--resource": "jobset",
        "--name": "training",
    }
    argv = [item for flag, value in pairs.items() if flag != missing for item in (flag, value)]
    with pytest.raises(SystemExit) as error:
        inspection.main(argv)
    assert error.value.code == 2
    assert setup.calls == []


@pytest.mark.parametrize(
    "status",
    [
        {},
        {"availableReplicas": 1, "observedGeneration": 1},
        # An old healthy replica cannot qualify the new desired controller image.
        {"replicas": 2, "updatedReplicas": 1, "availableReplicas": 1, "observedGeneration": 2},
    ],
)
def test_controller_without_current_availability_is_not_pass(setup, status):
    setup.args.controller_namespace = "operators"
    setup.args.controller_deployment = "controller"
    setup.replies["deployments.apps"] = {
        "metadata": {"uid": "deployment-uid", "generation": 2},
        "spec": {"replicas": 1},
        "status": status,
    }
    report = inspection.inspect_cluster(setup.args)
    assert report["checks"]["controller"]["outcome"] == "fail"
    assert report["checks"]["controller_provenance"]["outcome"] == "inconclusive"
