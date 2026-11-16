# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Reuse the Kubernetes foundation's owned launcher and staged runner fixtures."""

pytest_plugins = ["e2e_kubernetes.conftest"]
