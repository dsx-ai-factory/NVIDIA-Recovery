# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Provider-owned templates survive the production Helm/dotenv/settings boundary."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml
from attribution_adapter.settings.settings import Settings

CHART = Path(__file__).resolve().parents[2] / "deploy/helm/attribution-adapter"


def render_settings(tmp_path, values):
    """Render the real chart and extract its mounted settings file."""
    helm = shutil.which("helm")
    if helm is None:
        pytest.skip("Helm is required to exercise the deployed settings projection")
    values = {
        "loki": {"loki_cluster": "test-cluster"},
        "prometheus": {"prometheus_url": "http://metrics.example.com", "prometheus_cluster": "test-cluster"},
        **values,
    }
    result = subprocess.run(
        [helm, "template", "contract", str(CHART), "-f", "-"],
        input=yaml.safe_dump(values),
        text=True,
        capture_output=True,
        check=True,
    )
    configmap = next(
        document
        for document in yaml.safe_load_all(result.stdout)
        if document and document.get("kind") == "ConfigMap" and "settings.env" in document.get("data", {})
    )
    config = tmp_path / "settings.env"
    config.write_text(configmap["data"]["settings.env"])
    return config


@pytest.mark.parametrize("backend", ["loki", "victorialogs"])
def test_chart_preserves_native_query_text_in_runtime_provider(tmp_path, monkeypatch, backend):
    """Quotes, regex escapes, Unicode, newlines, and template braces must be lossless."""
    queries = {
        "application": "{{appName='app', regex=\"\\\\d+\\t\"}} | Unicode: café\n{job_id}",
        "system": "{component='journal'} |= `quoted \"text\" and \\literal`\n第二行 ${SET_QUERY_VAR} ${UNSET_QUERY_VAR}",
        "epilogue": "{{component='epilogue'}} |= 'job {job_id}' | regex=\"\\\\s+\"",
    }
    values = {
        "global": {
            "log_query": {
                "backend": backend,
                "providers": {
                    backend: {
                        "application": {"url": "http://application.example.com"},
                        "system": {"url": "http://system.example.com"},
                        "queries": {"attribution": queries},
                    }
                },
            }
        }
    }
    config = render_settings(tmp_path, values)
    monkeypatch.setattr(os, "environ", {})
    monkeypatch.setenv("SET_QUERY_VAR", "must-not-interpolate")

    settings = Settings(_env_file=config)

    assert list(settings.log_query_providers) == [backend]
    assert settings.log_query_providers[backend].queries.model_dump() == queries
    assert "applog_query=" not in config.read_text()
    assert "syslog_query=" not in config.read_text()


def test_configured_loki_job_templates_expand_without_interpreting_selector_braces(tmp_path, monkeypatch):
    """Operator templates preserve native predicates after job substitution."""
    config = render_settings(
        tmp_path,
        {
            "global": {
                "log_query": {
                    "providers": {
                        "loki": {
                            "application": {"url": "http://application.example.com"},
                            "system": {"url": "http://system.example.com"},
                            "queries": {
                                "attribution": {
                                    "application": "{{appName='slurm-app-log', service_name='test-cluster'}} |= `_{}_date_` | keep exporter, appName, service_name",
                                    "system": "{appName!='slurm-app-log', service_name='test-cluster', level!~'INFO|INFO2'} | keep appName, service_name",
                                    "epilogue": "{{appName!='slurm-app-log', service_name='test-cluster', level!~'INFO|INFO2'}} | keep appName, service_name",
                                }
                            },
                        }
                    }
                }
            }
        },
    )
    monkeypatch.setattr(os, "environ", {})
    queries = Settings(_env_file=config).log_query_providers["loki"].queries

    assert queries.application.format("12345") == (
        "{appName='slurm-app-log', service_name='test-cluster'} |= `_12345_date_` "
        "| keep exporter, appName, service_name"
    )
    expected_system = (
        "{appName!='slurm-app-log', service_name='test-cluster', level!~'INFO|INFO2'} | keep appName, service_name"
    )
    assert queries.epilogue.format("12345") == expected_system
    assert queries.system == expected_system


@pytest.mark.parametrize(
    "retired",
    [
        "applog_query",
        "syslog_query",
        "epilogue_query",
        "victorialogs_applog_query",
        "victorialogs_syslog_query",
        "victorialogs_epilogue_query",
        "logql_json_filter",
        "logql_syslog_index_filter",
        "logql_kernel_filter",
        "logql_applog_exclude_filter",
        "logql_prolog_exclude_filter",
        "logql_epilog_exclude_filter",
        "logql_applog_format",
    ],
)
@pytest.mark.parametrize("source", ["environment", "dotenv", "kwargs"])
def test_retired_query_locations_fail_with_migration_guidance(monkeypatch, tmp_path, retired, source):
    """Old template/filter inputs are never silently accepted."""
    monkeypatch.setattr(os, "environ", {})
    kwargs = {}
    if source == "environment":
        monkeypatch.setenv(retired.upper(), "legacy-query")
    elif source == "dotenv":
        config = tmp_path / "legacy.env"
        config.write_text(f"{retired}=legacy-query\n")
        kwargs["_env_file"] = config
    else:
        kwargs[retired] = "legacy-query"
    with pytest.raises(ValueError, match="Retired log-query templates or unused filters"):
        Settings(**kwargs)


@pytest.mark.parametrize(
    "queries",
    [
        {"application": "app", "system": "sys"},
        {"application": "", "system": "sys", "epilogue": "epilog"},
        {"application": "app", "system": "sys", "epilogue": "epilog", "typo": "bad"},
    ],
)
def test_runtime_provider_query_shape_is_strict(monkeypatch, queries):
    """Missing, empty, and unknown fields fail before reaching a query client."""
    monkeypatch.setattr(os, "environ", {})
    with pytest.raises(ValueError, match="log_query_providers.loki.queries"):
        Settings(log_query_providers={"loki": {"queries": queries}})
