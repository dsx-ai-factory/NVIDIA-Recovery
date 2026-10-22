#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# Generate Helm CRD templates at packaging time.
#
# This writes: <chart_dir>/templates/crds/crd.yaml
# and wraps the CRD YAML with:
#   {{- if .Values.installCRDs }} ... {{- end }}
#
# Usage:
#   helm.generate-crds.sh <project_dir> <chart_dir>
#
# Example (when run from inside $PROJECT_DIR):
#   bash hack/helm.generate-crds.sh "." "deploy/charts/${CHART_NAME}"

PROJECT_DIR="${1:?project dir is required}"
CHART_DIR="${2:?chart dir is required}"

CRD_TPL_DIR="${CHART_DIR}/templates/crds"
OUT="${CRD_TPL_DIR}/crd.yaml"

mkdir -p "${CRD_TPL_DIR}"

# Determine authoritative CRD source:
# - Prefer a bundled CRD file if present
# - Otherwise fall back to controller-gen output in config/crd/bases
CRD_BUNDLE="${PROJECT_DIR}/deploy/crds/bundle.yaml"
CRD_BASES_DIR="${PROJECT_DIR}/config/crd/bases"

tmp="$(mktemp)"
cleanup() { rm -f "${tmp}"; }
trap cleanup EXIT

cat > "${tmp}" <<'EOF'
{{- /*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0

Generated file: do not edit.

CRDs are installed when `.Values.installCRDs` is true.
*/ -}}
{{- if .Values.installCRDs }}
EOF

append_crd_file() {
  local f="$1"
  printf "\n---\n" >> "${tmp}"
  # Drop a leading '---' from the source to avoid double separators.
  awk 'NR==1 && $0=="---"{next} {print}' "${f}" >> "${tmp}"
}

if [ -f "${CRD_BUNDLE}" ]; then
  append_crd_file "${CRD_BUNDLE}"
elif [ -d "${CRD_BASES_DIR}" ]; then
  shopt -s nullglob
  files=( "${CRD_BASES_DIR}"/*.yaml "${CRD_BASES_DIR}"/*.yml )
  shopt -u nullglob
  if [ ${#files[@]} -eq 0 ]; then
    echo "No CRD YAML files found under ${CRD_BASES_DIR}" >&2
    exit 1
  fi
  for f in "${files[@]}"; do
    append_crd_file "${f}"
  done
else
  echo "No CRD source found. Expected either ${CRD_BUNDLE} or ${CRD_BASES_DIR}" >&2
  exit 1
fi

cat >> "${tmp}" <<'EOF'
{{- end }}
EOF

mv "${tmp}" "${OUT}"
