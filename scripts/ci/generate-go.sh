#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

ci_cd_root
ci_parse_go_packages "$@"

for package in "${ci_selected_go_packages[@]}"; do
    package_dir="$(ci_go_package_dir "$package")"
    case "$package" in
        api-gateway)
            buf="$(ci_buf_path)" || {
                echo "buf is required; run make install-go-tools" >&2
                exit 127
            }
            ci_verify_buf_version "$buf"
            ci_log "generating Go protobuf and OpenAPI artifacts for $package"
            (
                cd "$package_dir"
                "$buf" generate --include-imports
            )
            ;;
        advice-operator)
            ci_require_command go
            ci_load_go_tool_versions
            ci_log "generating Advice Operator mocks, API types, CRDs, and RBAC"
            (
                cd "$package_dir"
                go generate ./...
                go run "sigs.k8s.io/controller-tools/cmd/controller-gen@$CONTROLLER_GEN_VERSION" \
                    object:headerFile=hack/boilerplate.go.txt \
                    paths="./api/..."
                go run "sigs.k8s.io/controller-tools/cmd/controller-gen@$CONTROLLER_GEN_VERSION" \
                    crd rbac:roleName=manager-role paths="./..." \
                    output:crd:artifacts:config=config/crd/bases \
                    output:rbac:artifacts:config=config/rbac
            )
            # Keep the chart's conditional installation around the canonical CRD.
            {
                cat "$package_dir/hack/crd-header.tpl"
                cat "$package_dir/config/crd/bases/heimdall.nvidia.com_advices.yaml"
                printf '{{- end }}\n'
            } >deploy/helm/advice-operator/templates/crds/crd.yaml
            # The chart and Kustomize deployment share controller-generated rules.
            {
                cat "$package_dir/hack/rbac-header.tpl"
                awk '/^rules:/{rules=1; next} rules{print}' "$package_dir/config/rbac/role.yaml"
                printf '{{- end }}\n'
            } >deploy/helm/advice-operator/templates/_manager-rbac.tpl
            ;;
        slurm-advisor-controller)
            ci_require_command go
            ci_log "running go generate for $package"
            (
                cd "$package_dir"
                go generate ./...
            )
            ;;
    esac
done
