#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

ci_cd_root

fail() {
    echo "nightly test selection contract failed: $*" >&2
    exit 1
}

test_tmp="$(mktemp -d)"
trap 'rm -rf "$test_tmp"' EXIT
fake_bin="$test_tmp/fake-bin"
tool_log="$test_tmp/tools.log"
mkdir -p "$fake_bin"

cat >"$fake_bin/fake-tool" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

tool="${0##*/}"
if [[ "$tool" == "git" && "${1:-}" == "rev-parse" && "${2:-}" == "--show-toplevel" ]]; then
    printf '%s\n' "$FAKE_REPO_ROOT"
    exit 0
fi

if [[ "$tool" == "uv" ]]; then
    printf 'env\tPYTEST_ADDOPTS=%s\n' "${PYTEST_ADDOPTS-<unset>}" >>"$FAKE_TOOL_LOG"
fi

{
    printf '%s' "$tool"
    for arg in "$@"; do
        printf '\t%s' "$arg"
    done
    printf '\n'
} >>"$FAKE_TOOL_LOG"
EOF
chmod +x "$fake_bin/fake-tool"
for tool in git go uv; do
    ln -s fake-tool "$fake_bin/$tool"
done

repo_root="$(ci_repo_root)"
export FAKE_REPO_ROOT="$repo_root"
export FAKE_TOOL_LOG="$tool_log"

: >"$tool_log"
(
    unset GO_TEST_ARGS
    GITHUB_ACTIONS="true"
    GITHUB_REF_NAME="pull-request/123"
    export GITHUB_ACTIONS GITHUB_REF_NAME
    PATH="$fake_bin:$PATH" scripts/ci/test-go.sh --package api-gateway >/dev/null
)
grep -Fx $'go\ttest\t./...' "$tool_log" >/dev/null ||
    fail "Go runner inferred test selection from the CI provider environment"

: >"$tool_log"
(
    GO_TEST_ARGS="-skip ^TestNightly"
    export GO_TEST_ARGS
    PATH="$fake_bin:$PATH" scripts/ci/test-go.sh --package api-gateway >/dev/null
)
grep -Fx $'go\ttest\t./...\t-skip\t^TestNightly' "$tool_log" >/dev/null ||
    fail "Go runner did not forward the explicit nightly selection argument"

: >"$tool_log"
(
    unset PYTEST_ADDOPTS
    GITHUB_ACTIONS="true"
    GITHUB_REF_NAME="pull-request/123"
    export GITHUB_ACTIONS GITHUB_REF_NAME
    PATH="$fake_bin:$PATH" scripts/ci/test-python.sh --package watcher >/dev/null
)
grep -F $'env\tPYTEST_ADDOPTS=<unset>' "$tool_log" >/dev/null ||
    fail "Python runner inferred test selection from the CI provider environment"

: >"$tool_log"
(
    PYTEST_ADDOPTS="-m 'not nightly'"
    export PYTEST_ADDOPTS
    PATH="$fake_bin:$PATH" scripts/ci/test-python.sh --package watcher >/dev/null
)
grep -F "PYTEST_ADDOPTS=-m 'not nightly'" "$tool_log" >/dev/null ||
    fail "Python unit runner did not preserve the explicit nightly selection argument"

: >"$tool_log"
(
    PYTEST_ADDOPTS="-m 'not nightly'"
    export PYTEST_ADDOPTS
    PATH="$fake_bin:$PATH" scripts/ci/test-python-integration.sh \
        --package notifications >/dev/null
)
grep -F "PYTEST_ADDOPTS=-m 'not nightly'" "$tool_log" >/dev/null ||
    fail "Python integration runner did not preserve the explicit nightly selection argument"

for config in \
    pyproject.toml \
    anomaly_detection_isolator/pyproject.toml \
    attribution_adapter/pyproject.toml \
    job_manager/pyproject.toml \
    notifications/pyproject.toml \
    recovery_libs/pyproject.toml \
    watcher/pyproject.toml; do
    grep -F 'nightly: tests excluded from pull requests and main pushes; included in nightly and tagged-release suites' "$config" >/dev/null ||
        fail "$config does not register the nightly pytest marker"
done

if grep -E 'ci_is_review_pipeline|ci_configure_pytest' \
    scripts/ci/lib.sh \
    scripts/ci/test-go.sh \
    scripts/ci/test-python.sh \
    scripts/ci/test-python-integration.sh \
    scripts/ci/e2e/run-tests.sh >/dev/null; then
    fail "a generic test runner still owns review-pipeline selection"
fi

grep -F 'GO_TEST_ARGS: "-coverprofile=coverage.out -skip ^TestNightly"' \
    .github/workflows/pull-request-and-main.yml >/dev/null ||
    fail "GitHub pull-request and main workflow does not exclude nightly Go tests"
grep -F "PYTEST_ADDOPTS: '-m \"not nightly\"'" \
    .github/workflows/pull-request-and-main.yml >/dev/null ||
    fail "GitHub pull-request and main workflow does not exclude nightly Python tests"
grep -F 'skip_nightly: true' .github/workflows/e2e-review.yml >/dev/null ||
    fail "GitHub review E2E workflow does not exclude nightly tests"
grep -F 'skip_nightly: true' .github/workflows/e2e-main.yml >/dev/null ||
    fail "GitHub main E2E workflow does not exclude nightly tests"
grep -A3 '^      skip_nightly:' .github/workflows/e2e-base.yml |
    grep -F 'default: false' >/dev/null ||
    fail "GitHub reusable E2E workflow does not default to the full suite"
grep -F "PYTEST_ADDOPTS: \${{ inputs.skip_nightly && '-m \"not nightly\"' || '' }}" \
    .github/workflows/e2e-base.yml >/dev/null ||
    fail "GitHub reusable E2E workflow does not apply its explicit nightly exclusion"
for workflow in .github/workflows/nightly.yml .github/workflows/tag-release.yml; do
    if grep -F 'skip_nightly: true' "$workflow" >/dev/null; then
        fail "$workflow excludes nightly tests from a full-suite pipeline"
    fi
done

echo "ok: CI workflows enforce the nightly selection matrix"
