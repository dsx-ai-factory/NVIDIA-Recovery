#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

ci_repo_root() {
    git rev-parse --show-toplevel
}

ci_cd_root() {
    cd "$(ci_repo_root)"
}

ci_log() {
    printf 'ci: %s\n' "$*" >&2
}

ci_configure_uv() {
    local repo_root
    repo_root="$(ci_repo_root)"
    export UV_CACHE_DIR="${UV_CACHE_DIR:-$repo_root/.uv-cache}"
    export UV_LINK_MODE="${UV_LINK_MODE:-copy}"
}

ci_parse_python_check_args() {
    ci_fix=false
    ci_paths=()

    while (($# > 0)); do
        case "$1" in
            --fix)
                ci_fix=true
                shift
                ;;
            --)
                shift
                ci_paths+=("$@")
                break
                ;;
            -*)
                echo "unsupported option: $1" >&2
                exit 2
                ;;
            *)
                ci_paths+=("$1")
                shift
                ;;
        esac
    done
}

ci_resolve_python_targets() {
    ci_targets=()

    if ((${#ci_paths[@]} == 0)); then
        ci_targets=(".")
        return 0
    fi

    local path
    for path in "${ci_paths[@]}"; do
        if [[ -f "$path" && "$path" == *.py ]]; then
            ci_targets+=("$path")
        fi
    done

    ((${#ci_targets[@]} > 0))
}

ci_python_packages() {
    local repo_root
    repo_root="$(ci_repo_root)"
    "$repo_root/scripts/ci/list-python-packages.sh"
}

ci_package_dir() {
    case "$1" in
        recovery-libs) echo "recovery_libs" ;;
        job-manager) echo "job_manager" ;;
        watcher) echo "watcher" ;;
        anomaly-detection-isolator) echo "anomaly_detection_isolator" ;;
        attribution-adapter) echo "attribution_adapter" ;;
        notifications) echo "notifications" ;;
        *)
            echo "unsupported Python package: $1" >&2
            return 2
            ;;
    esac
}

ci_report_dir() {
    local dir="reports/python/$1"
    mkdir -p "$dir"
    echo "$dir"
}

ci_go_packages() {
    # Return all supported Go modules for the default verification set.
    printf '%s\n' \
        "api-gateway" \
        "slurm-advisor-controller"
}

ci_go_package_dir() {
    case "$1" in
        api-gateway) echo "api-gateway" ;;
        slurm-advisor-controller) echo "slurm-advisor-controller" ;;
        *)
            echo "unsupported Go package: $1" >&2
            return 2
            ;;
    esac
}

ci_parse_go_packages() {
    ci_selected_go_packages=()

    while (($# > 0)); do
        case "$1" in
            --package)
                if (($# < 2)); then
                    echo "missing value for --package" >&2
                    return 2
                fi
                ci_selected_go_packages+=("$2")
                shift 2
                ;;
            --)
                shift
                if (($# > 0)); then
                    echo "unexpected positional arguments: $*" >&2
                    return 2
                fi
                ;;
            -*)
                echo "unsupported option: $1" >&2
                return 2
                ;;
            *)
                echo "unexpected positional argument: $1" >&2
                return 2
                ;;
        esac
    done

    if ((${#ci_selected_go_packages[@]} == 0)); then
        while IFS= read -r package; do
            ci_selected_go_packages+=("$package")
        done < <(ci_go_packages)
    fi
}

ci_load_go_tool_versions() {
    local versions_file
    versions_file="$(ci_repo_root)/scripts/ci/go-tool-versions.env"
    if [[ ! -f "$versions_file" ]]; then
        echo "missing Go tool versions file: $versions_file" >&2
        return 1
    fi

    # shellcheck source=go-tool-versions.env
    source "$versions_file"
    : "${GOLANGCI_LINT_VERSION:?GOLANGCI_LINT_VERSION is required}"
    : "${BUF_VERSION:?BUF_VERSION is required}"
}

ci_go_tools_bin() {
    printf '%s\n' "${GO_TOOLS_BIN:-$(ci_repo_root)/.cache/go-tools/bin}"
}

ci_go_generated_files() {
    local package_dir="$1"

    {
        git grep -l '^// Code generated .* DO NOT EDIT\.$' HEAD -- "$package_dir" 2>/dev/null |
            sed 's/^HEAD://' || true
        find "$package_dir" -type f -name '*.go' \
            -exec grep -l '^// Code generated .* DO NOT EDIT\.$' {} + 2>/dev/null || true
    } | sort -u
}

ci_check_no_generated_drift() {
    local package="$1"
    shift

    local status
    status="$(git status --porcelain --untracked-files=all -- "$@")"
    if [[ -z "$status" ]]; then
        printf 'ok: generated artifacts are current for %s\n' "$package"
        return 0
    fi

    echo "generated artifacts are out of sync for $package." >&2
    echo "Run the generator locally and commit the resulting artifacts." >&2
    echo >&2
    echo "$status" >&2
    return 1
}

ci_golangci_lint_path() {
    local tools_bin
    tools_bin="$(ci_go_tools_bin)"
    if [[ -x "$tools_bin/golangci-lint" ]]; then
        printf '%s\n' "$tools_bin/golangci-lint"
        return 0
    fi

    command -v golangci-lint
}

ci_buf_path() {
    local tools_bin
    tools_bin="$(ci_go_tools_bin)"
    if [[ -x "$tools_bin/buf" ]]; then
        printf '%s\n' "$tools_bin/buf"
        return 0
    fi

    command -v buf
}

ci_verify_buf_version() {
    local buf="$1"
    local expected_version actual

    ci_load_go_tool_versions
    expected_version="${BUF_VERSION#v}"
    actual="$("$buf" --version)"
    if [[ "$actual" != "$expected_version" ]]; then
        echo "buf $expected_version is required; found: $actual" >&2
        return 1
    fi
}

ci_verify_golangci_lint_version() {
    local golangci_lint="$1"
    local expected_version actual

    ci_load_go_tool_versions
    expected_version="${GOLANGCI_LINT_VERSION#v}"
    actual="$("$golangci_lint" --version)"
    if [[ "$actual" != *" version $expected_version "* ]]; then
        echo "golangci-lint $expected_version is required; found: $actual" >&2
        return 1
    fi
}

ci_typescript_package_dir() {
    echo "dashboard"
}

ci_require_command() {
    local command_name="$1"
    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "$command_name is required" >&2
        return 127
    fi
}

ci_assert_lint_left_tracked_files_clean() {
    local label="$1"
    shift

    if [[ "${CI:-}" != "true" ]]; then
        return 0
    fi

    local status
    status="$(git status --porcelain --untracked-files=no -- "$@")"
    if [[ -z "$status" ]]; then
        return 0
    fi

    echo "CI lint modified tracked files for $label." >&2
    echo "Run the relevant formatter or lint fixer locally, then commit the result." >&2
    echo "$status" >&2
    return 1
}
