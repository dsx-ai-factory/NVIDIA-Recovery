#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

list_python_packages() {
    cat <<'PACKAGES'
recovery-libs
job-manager
watcher
anomaly-detection-isolator
attribution-adapter
notifications
PACKAGES
}

json=false
while (($# > 0)); do
    case "$1" in
        --json)
            json=true
            shift
            ;;
        --)
            shift
            if (($# > 0)); then
                echo "unexpected positional arguments: $*" >&2
                exit 2
            fi
            ;;
        -*)
            echo "unsupported option: $1" >&2
            exit 2
            ;;
        *)
            echo "unexpected positional argument: $1" >&2
            exit 2
            ;;
    esac
done

if [[ "$json" != true ]]; then
    list_python_packages
    exit 0
fi

first=true
printf '['
while IFS= read -r package; do
    if [[ -z "$package" ]]; then
        continue
    fi
    if [[ "$first" == true ]]; then
        first=false
    else
        printf ','
    fi
    printf '"%s"' "$package"
done < <(list_python_packages)
printf ']\n'
